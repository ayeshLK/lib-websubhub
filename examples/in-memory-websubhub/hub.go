// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

const (
	defaultUpdateQueue   = 256
	defaultDeliveryQueue = 1024
	defaultWorkers       = 4
	defaultMaxTopicBody  = 1 << 20 // 1 MiB
)

type hubOptions struct {
	hubURL          string
	updateQueue     int
	deliveryQueue   int
	deliveryWorkers int
	maxTopicBody    int64
	httpClient      *http.Client
	logger          *slog.Logger
}

type storedSubscription struct {
	subscription websubhub.Subscription
	expiresAt    time.Time
	version      uint64
}

type deliveryJob struct {
	topic   string
	stored  storedSubscription
	content websubhub.ContentDistribution
}

// memoryHub owns all application policy and state. The websubhub package owns
// HTTP parsing, intent verification, protocol responses, and delivery framing.
type memoryHub struct {
	mu            sync.RWMutex
	topics        map[string]struct{}
	subscriptions map[string]map[string]storedSubscription
	nextVersion   uint64

	updates    chan websubhub.UpdateMessage
	deliveries chan deliveryJob
	queueMu    sync.RWMutex
	closed     bool
	closeOnce  sync.Once
	done       chan struct{}

	ctx             context.Context
	cancel          context.CancelFunc
	dispatcherWG    sync.WaitGroup
	deliveryWG      sync.WaitGroup
	deliveryConfig  websubhub.DeliveryConfig
	topicHTTPClient *http.Client
	maxTopicBody    int64
	logger          *slog.Logger
}

func newMemoryHub(options hubOptions) (*memoryHub, error) {
	if options.hubURL == "" {
		return nil, errors.New("hub URL is required")
	}
	if options.updateQueue <= 0 {
		options.updateQueue = defaultUpdateQueue
	}
	if options.deliveryQueue <= 0 {
		options.deliveryQueue = defaultDeliveryQueue
	}
	if options.deliveryWorkers <= 0 {
		options.deliveryWorkers = defaultWorkers
	}
	if options.maxTopicBody <= 0 {
		options.maxTopicBody = defaultMaxTopicBody
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}
	if options.httpClient == nil {
		options.httpClient = &http.Client{Timeout: 15 * time.Second}
	}

	ctx, cancel := context.WithCancel(context.Background())
	hub := &memoryHub{
		topics:          make(map[string]struct{}),
		subscriptions:   make(map[string]map[string]storedSubscription),
		updates:         make(chan websubhub.UpdateMessage, options.updateQueue),
		deliveries:      make(chan deliveryJob, options.deliveryQueue),
		done:            make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
		deliveryConfig:  websubhub.DeliveryConfig{HTTPClient: options.httpClient, Timeout: 15 * time.Second},
		topicHTTPClient: options.httpClient,
		maxTopicBody:    options.maxTopicBody,
		logger:          options.logger,
	}

	hub.dispatcherWG.Add(1)
	go hub.dispatch()
	for range options.deliveryWorkers {
		hub.deliveryWG.Add(1)
		go hub.deliver()
	}
	go func() {
		hub.dispatcherWG.Wait()
		hub.deliveryWG.Wait()
		close(hub.done)
	}()
	return hub, nil
}

func (h *memoryHub) service() websubhub.Service {
	return websubhub.Service{
		OnRegisterTopic:            h.onRegisterTopic,
		OnDeregisterTopic:          h.onDeregisterTopic,
		OnUpdateMessage:            h.onUpdateMessage,
		OnSubscriptionValidation:   h.onSubscriptionValidation,
		OnSubscriptionVerified:     h.onSubscriptionVerified,
		OnUnsubscriptionValidation: h.onUnsubscriptionValidation,
		OnUnsubscriptionVerified:   h.onUnsubscriptionVerified,
	}
}

func (h *memoryHub) onRegisterTopic(_ context.Context, registration websubhub.TopicRegistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.topics[registration.Topic]; exists {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is already registered"}
	}
	h.topics[registration.Topic] = struct{}{}
	h.logger.Info("topic registered", "topic", registration.Topic)
	return websubhub.Result{}, nil
}

func (h *memoryHub) onDeregisterTopic(_ context.Context, deregistration websubhub.TopicDeregistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.topics[deregistration.Topic]; !exists {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	delete(h.topics, deregistration.Topic)
	delete(h.subscriptions, deregistration.Topic)
	h.logger.Info("topic deregistered", "topic", deregistration.Topic)
	return websubhub.Result{}, nil
}

func (h *memoryHub) onUpdateMessage(_ context.Context, update websubhub.UpdateMessage, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	if !h.hasTopic(update.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is not registered"}
	}

	update.Body = append([]byte(nil), update.Body...)
	update.Header = update.Header.Clone()
	h.queueMu.RLock()
	defer h.queueMu.RUnlock()
	if h.closed {
		return unavailable("hub is shutting down"), nil
	}
	select {
	case h.updates <- update:
		return websubhub.Result{}, nil
	default:
		return unavailable("update queue is full"), nil
	}
}

func (h *memoryHub) onSubscriptionValidation(_ context.Context, subscription websubhub.Subscription, _ websubhub.RequestMetadata) error {
	if !h.hasTopic(subscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	// An existing callback/topic pair is intentionally allowed. A successfully
	// verified request renews and replaces the previous subscription.
	return nil
}

func (h *memoryHub) onSubscriptionVerified(_ context.Context, verified websubhub.VerifiedSubscription, _ websubhub.RequestMetadata) error {
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 {
		return fmt.Errorf("invalid effective lease %q", verified.EffectiveLeaseSeconds)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.topics[verified.Topic]; !exists {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	byCallback := h.subscriptions[verified.Topic]
	if byCallback == nil {
		byCallback = make(map[string]storedSubscription)
		h.subscriptions[verified.Topic] = byCallback
	}
	h.nextVersion++
	byCallback[verified.Callback] = storedSubscription{
		subscription: cloneSubscription(verified.Subscription),
		expiresAt:    verified.LeaseStartedAt.Add(time.Duration(leaseSeconds) * time.Second),
		version:      h.nextVersion,
	}
	h.logger.Info("subscription verified", "topic", verified.Topic, "callback", verified.Callback, "lease_seconds", leaseSeconds)
	return nil
}

func (h *memoryHub) onUnsubscriptionValidation(_ context.Context, unsubscription websubhub.Unsubscription, _ websubhub.RequestMetadata) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if _, exists := h.topics[unsubscription.Topic]; !exists {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if _, exists := h.subscriptions[unsubscription.Topic][unsubscription.Callback]; !exists {
		return &websubhub.DeniedError{Reason: "subscription does not exist"}
	}
	return nil
}

func (h *memoryHub) onUnsubscriptionVerified(_ context.Context, verified websubhub.VerifiedUnsubscription, _ websubhub.RequestMetadata) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	byCallback := h.subscriptions[verified.Topic]
	delete(byCallback, verified.Callback)
	if len(byCallback) == 0 {
		delete(h.subscriptions, verified.Topic)
	}
	h.logger.Info("unsubscription verified", "topic", verified.Topic, "callback", verified.Callback)
	return nil
}

func (h *memoryHub) dispatch() {
	defer h.dispatcherWG.Done()
	defer close(h.deliveries)
	for update := range h.updates {
		content, err := h.contentFor(update)
		if err != nil {
			h.logger.Error("update could not be materialized", "topic", update.Topic, "error", err)
			continue
		}
		for _, stored := range h.activeSubscriptions(update.Topic, time.Now()) {
			job := deliveryJob{topic: update.Topic, stored: stored, content: content}
			select {
			case h.deliveries <- job:
			case <-h.ctx.Done():
				return
			}
		}
	}
}

func (h *memoryHub) deliver() {
	defer h.deliveryWG.Done()
	for job := range h.deliveries {
		client, err := websubhub.NewDeliveryClient(job.stored.subscription, h.deliveryConfig)
		if err != nil {
			h.logger.Error("delivery client could not be created", "callback", job.stored.subscription.Callback, "error", err)
			continue
		}
		response, err := client.Deliver(h.ctx, job.content)
		if err == nil {
			h.logger.Info("content delivered", "topic", job.topic, "callback", job.stored.subscription.Callback, "status", response.StatusCode)
			continue
		}
		if errors.Is(err, websubhub.ErrSubscriptionGone) {
			h.removeSubscription(job.topic, job.stored.subscription.Callback, job.stored.version)
			h.logger.Info("subscription removed after HTTP 410", "topic", job.topic, "callback", job.stored.subscription.Callback)
			continue
		}
		h.logger.Error("content delivery failed", "topic", job.topic, "callback", job.stored.subscription.Callback, "error", err)
	}
}

func (h *memoryHub) contentFor(update websubhub.UpdateMessage) (websubhub.ContentDistribution, error) {
	if update.Kind == websubhub.UpdateContent {
		return websubhub.ContentDistribution{
			ContentType: update.ContentType,
			Body:        append([]byte(nil), update.Body...),
		}, nil
	}

	request, err := http.NewRequestWithContext(h.ctx, http.MethodGet, update.Topic, nil)
	if err != nil {
		return websubhub.ContentDistribution{}, err
	}
	response, err := h.topicHTTPClient.Do(request)
	if err != nil {
		return websubhub.ContentDistribution{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return websubhub.ContentDistribution{}, fmt.Errorf("topic returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, h.maxTopicBody+1))
	if err != nil {
		return websubhub.ContentDistribution{}, err
	}
	if int64(len(body)) > h.maxTopicBody {
		return websubhub.ContentDistribution{}, fmt.Errorf("topic response exceeds %d bytes", h.maxTopicBody)
	}
	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return websubhub.ContentDistribution{
		ContentType: contentType,
		Body:        body,
	}, nil
}

func (h *memoryHub) activeSubscriptions(topic string, now time.Time) []storedSubscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	byCallback := h.subscriptions[topic]
	active := make([]storedSubscription, 0, len(byCallback))
	for callback, stored := range byCallback {
		if !stored.expiresAt.After(now) {
			delete(byCallback, callback)
			continue
		}
		active = append(active, stored)
	}
	if len(byCallback) == 0 {
		delete(h.subscriptions, topic)
	}
	return active
}

func (h *memoryHub) removeSubscription(topic, callback string, version uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	byCallback := h.subscriptions[topic]
	if stored, exists := byCallback[callback]; exists && stored.version == version {
		delete(byCallback, callback)
	}
	if len(byCallback) == 0 {
		delete(h.subscriptions, topic)
	}
}

func (h *memoryHub) hasTopic(topic string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, exists := h.topics[topic]
	return exists
}

func (h *memoryHub) hasSubscription(topic, callback string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	stored, exists := h.subscriptions[topic][callback]
	return exists && stored.expiresAt.After(time.Now())
}

func (h *memoryHub) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		h.queueMu.Lock()
		h.closed = true
		close(h.updates)
		h.queueMu.Unlock()
	})
	select {
	case <-h.done:
		h.cancel()
		return nil
	case <-ctx.Done():
		h.cancel()
		return ctx.Err()
	}
}

func unavailable(message string) websubhub.Result {
	return websubhub.Result{
		StatusCode:  http.StatusServiceUnavailable,
		ContentType: "text/plain; charset=utf-8",
		Body:        []byte(message + "\n"),
	}
}

func cloneSubscription(subscription websubhub.Subscription) websubhub.Subscription {
	subscription.Parameters = cloneValues(subscription.Parameters)
	return subscription
}

func cloneValues(values url.Values) url.Values {
	if values == nil {
		return nil
	}
	cloned := make(url.Values, len(values))
	for key, entries := range values {
		cloned[key] = append([]string(nil), entries...)
	}
	return cloned
}
