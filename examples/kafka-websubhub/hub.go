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
	"strconv"
	"sync"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

const (
	defaultDeliveryAttempts = 3
	defaultRetryBackoff     = time.Second
	defaultMaxTopicBody     = 1 << 20 // 1 MiB
)

type hubOptions struct {
	hubURL           string
	broker           messageBroker
	deliveryAttempts int
	retryBackoff     time.Duration
	maxTopicBody     int64
	httpClient       *http.Client
	logger           *slog.Logger
}

type updateEvent struct {
	Topic       string `json:"topic"`
	ContentType string `json:"content_type"`
	Body        []byte `json:"body"`
}

type deadLetter struct {
	RecordID string      `json:"record_id"`
	Attempts int         `json:"attempts"`
	Reason   string      `json:"reason"`
	Update   updateEvent `json:"update"`
}

type storedSubscription struct {
	subscription websubhub.Subscription
	expiresAt    time.Time
	version      uint64
}

// kafkaHub is application code: it owns Kafka persistence, lease expiry,
// fan-out, retries, dead-lettering, and delivery policy.
type kafkaHub struct {
	operationMu sync.Mutex
	mu          sync.RWMutex
	topics      map[string]struct{}
	subscribers map[string]map[string]storedSubscription
	nextVersion uint64

	broker           messageBroker
	deliveryConfig   websubhub.DeliveryConfig
	deliveryAttempts int
	retryBackoff     time.Duration
	maxTopicBody     int64
	topicHTTPClient  *http.Client
	logger           *slog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	closeOnce sync.Once
	done      chan struct{}
	errors    chan error
}

func newKafkaHub(startupContext context.Context, options hubOptions) (*kafkaHub, error) {
	if options.hubURL == "" {
		return nil, errors.New("hub URL is required")
	}
	if options.broker == nil {
		return nil, errors.New("message broker is required")
	}
	if options.deliveryAttempts <= 0 {
		options.deliveryAttempts = defaultDeliveryAttempts
	}
	if options.retryBackoff < 0 {
		return nil, errors.New("retry backoff must not be negative")
	}
	if options.retryBackoff == 0 {
		options.retryBackoff = defaultRetryBackoff
	}
	if options.maxTopicBody <= 0 {
		options.maxTopicBody = defaultMaxTopicBody
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}
	if options.httpClient == nil {
		options.httpClient = &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	hub := &kafkaHub{
		topics:           make(map[string]struct{}),
		subscribers:      make(map[string]map[string]storedSubscription),
		broker:           options.broker,
		deliveryConfig:   websubhub.DeliveryConfig{HTTPClient: options.httpClient, Timeout: 15 * time.Second},
		deliveryAttempts: options.deliveryAttempts,
		retryBackoff:     options.retryBackoff,
		maxTopicBody:     options.maxTopicBody,
		topicHTTPClient:  options.httpClient,
		logger:           options.logger,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		errors:           make(chan error, 1),
	}
	if err := hub.broker.ReplayEvents(startupContext, hub); err != nil {
		cancel()
		hub.broker.Close()
		return nil, err
	}

	hub.workers.Add(2)
	go hub.runWorker("event consumer", func() error {
		return hub.broker.ConsumeEvents(hub.ctx, hub)
	})
	go hub.runWorker("update consumer", func() error {
		return hub.broker.ConsumeUpdates(hub.ctx, hub.consumeUpdate)
	})
	go func() {
		hub.workers.Wait()
		close(hub.done)
	}()
	return hub, nil
}

func (h *kafkaHub) service() websubhub.Service {
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

func (h *kafkaHub) runWorker(name string, run func() error) {
	defer h.workers.Done()
	if err := run(); err != nil && h.ctx.Err() == nil {
		select {
		case h.errors <- fmt.Errorf("%s: %w", name, err):
		default:
		}
		h.cancel()
	}
}

func (h *kafkaHub) errorsChannel() <-chan error {
	return h.errors
}

func (h *kafkaHub) onRegisterTopic(ctx context.Context, registration websubhub.TopicRegistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if h.hasTopic(registration.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is already registered"}
	}
	if err := h.broker.PublishTopicRegistration(ctx, registration); err != nil {
		return websubhub.Result{}, err
	}
	h.logger.Info("topic registration event published", "topic", registration.Topic)
	return websubhub.Result{}, nil
}

func (h *kafkaHub) onDeregisterTopic(ctx context.Context, deregistration websubhub.TopicDeregistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if !h.hasTopic(deregistration.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if err := h.broker.PublishTopicDeregistration(ctx, deregistration); err != nil {
		return websubhub.Result{}, err
	}
	h.logger.Info("topic deregistration event published", "topic", deregistration.Topic)
	return websubhub.Result{}, nil
}

func (h *kafkaHub) onUpdateMessage(ctx context.Context, update websubhub.UpdateMessage, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	if !h.hasTopic(update.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	event, err := h.materializeUpdate(ctx, update)
	if err != nil {
		return websubhub.Result{}, err
	}
	if err := h.broker.PublishUpdate(ctx, event); err != nil {
		return websubhub.Result{}, err
	}
	return websubhub.Result{}, nil
}

func (h *kafkaHub) onSubscriptionValidation(_ context.Context, subscription websubhub.Subscription, _ websubhub.RequestMetadata) error {
	if !h.hasTopic(subscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	return nil
}

func (h *kafkaHub) onSubscriptionVerified(ctx context.Context, verified websubhub.VerifiedSubscription, _ websubhub.RequestMetadata) error {
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 {
		return fmt.Errorf("invalid effective lease %q", verified.EffectiveLeaseSeconds)
	}

	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if !h.hasTopic(verified.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if err := h.broker.PublishSubscription(ctx, verified); err != nil {
		return err
	}
	h.logger.Info("subscription event published", "topic", verified.Topic, "lease_seconds", leaseSeconds)
	return nil
}

func (h *kafkaHub) onUnsubscriptionValidation(_ context.Context, unsubscription websubhub.Unsubscription, _ websubhub.RequestMetadata) error {
	if !h.hasTopic(unsubscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if !h.hasSubscription(unsubscription.Topic, unsubscription.Callback) {
		return &websubhub.DeniedError{Reason: "subscription does not exist"}
	}
	return nil
}

func (h *kafkaHub) onUnsubscriptionVerified(ctx context.Context, verified websubhub.VerifiedUnsubscription, _ websubhub.RequestMetadata) error {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if err := h.broker.PublishUnsubscription(ctx, verified); err != nil {
		return err
	}
	h.logger.Info("unsubscription event published", "topic", verified.Topic)
	return nil
}

func (h *kafkaHub) applyTopicRegistration(_ context.Context, registration websubhub.TopicRegistration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if registration.Mode != websubhub.ModeRegister || registration.Topic == "" {
		return errors.New("invalid topic registration event")
	}
	h.topics[registration.Topic] = struct{}{}
	h.logger.Info("state event applied", "mode", registration.Mode, "topic", registration.Topic)
	return nil
}

func (h *kafkaHub) applyTopicDeregistration(_ context.Context, deregistration websubhub.TopicDeregistration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if deregistration.Mode != websubhub.ModeDeregister || deregistration.Topic == "" {
		return errors.New("invalid topic deregistration event")
	}
	delete(h.topics, deregistration.Topic)
	delete(h.subscribers, deregistration.Topic)
	h.logger.Info("state event applied", "mode", deregistration.Mode, "topic", deregistration.Topic)
	return nil
}

func (h *kafkaHub) applySubscription(_ context.Context, verified websubhub.VerifiedSubscription) error {
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 || verified.Mode != websubhub.ModeSubscribe || verified.Topic == "" || verified.Callback == "" || verified.Hub == "" || verified.LeaseStartedAt.IsZero() {
		return errors.New("invalid verified subscription event")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextVersion++
	byCallback := h.subscribers[verified.Topic]
	if byCallback == nil {
		byCallback = make(map[string]storedSubscription)
		h.subscribers[verified.Topic] = byCallback
	}
	byCallback[verified.Callback] = storedSubscription{
		subscription: verified.Subscription,
		expiresAt:    verified.LeaseStartedAt.Add(time.Duration(leaseSeconds) * time.Second),
		version:      h.nextVersion,
	}
	h.logger.Info("state event applied", "mode", verified.Mode, "topic", verified.Topic)
	return nil
}

func (h *kafkaHub) applyUnsubscription(_ context.Context, verified websubhub.VerifiedUnsubscription) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if verified.Mode != websubhub.ModeUnsubscribe || verified.Topic == "" || verified.Callback == "" {
		return errors.New("invalid verified unsubscription event")
	}
	byCallback := h.subscribers[verified.Topic]
	delete(byCallback, verified.Callback)
	if len(byCallback) == 0 {
		delete(h.subscribers, verified.Topic)
	}
	h.logger.Info("state event applied", "mode", verified.Mode, "topic", verified.Topic)
	return nil
}

func (h *kafkaHub) materializeUpdate(ctx context.Context, update websubhub.UpdateMessage) (updateEvent, error) {
	if update.Kind == websubhub.UpdateContent {
		return updateEvent{Topic: update.Topic, ContentType: update.ContentType, Body: append([]byte(nil), update.Body...)}, nil
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, update.Topic, nil)
	if err != nil {
		return updateEvent{}, err
	}
	response, err := h.topicHTTPClient.Do(request)
	if err != nil {
		return updateEvent{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return updateEvent{}, fmt.Errorf("topic returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, h.maxTopicBody+1))
	if err != nil {
		return updateEvent{}, err
	}
	if int64(len(body)) > h.maxTopicBody {
		return updateEvent{}, fmt.Errorf("topic response exceeds %d bytes", h.maxTopicBody)
	}
	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return updateEvent{Topic: update.Topic, ContentType: contentType, Body: body}, nil
}

func (h *kafkaHub) consumeUpdate(ctx context.Context, recordID string, update updateEvent) error {
	var lastFailure error
	for attempt := 1; attempt <= h.deliveryAttempts; attempt++ {
		lastFailure = h.deliverUpdate(ctx, update)
		if lastFailure == nil {
			return nil
		}
		if attempt < h.deliveryAttempts {
			if err := waitContext(ctx, h.retryBackoff); err != nil {
				return err
			}
		}
	}

	h.logger.Error("content delivery exhausted retries", "topic", update.Topic, "record_id", recordID, "attempts", h.deliveryAttempts)
	return h.broker.PublishDeadLetter(ctx, deadLetter{
		RecordID: recordID,
		Attempts: h.deliveryAttempts,
		Reason:   "one or more subscriber deliveries failed",
		Update:   update,
	})
}

func (h *kafkaHub) deliverUpdate(ctx context.Context, update updateEvent) error {
	subscriptions := h.activeSubscriptions(update.Topic, time.Now())
	failed := false
	for _, stored := range subscriptions {
		client, err := websubhub.NewDeliveryClient(stored.subscription, h.deliveryConfig)
		if err == nil {
			_, err = client.Deliver(ctx, websubhub.ContentDistribution{
				ContentType: update.ContentType,
				Body:        append([]byte(nil), update.Body...),
			})
		}
		if err == nil {
			continue
		}
		if errors.Is(err, websubhub.ErrSubscriptionGone) {
			if removeErr := h.removeGone(ctx, update.Topic, stored.subscription.Callback, stored.version); removeErr != nil {
				failed = true
			}
			continue
		}
		failed = true
	}
	if failed {
		return errors.New("one or more subscriber deliveries failed")
	}
	return nil
}

func (h *kafkaHub) removeGone(ctx context.Context, topic, callback string, version uint64) error {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	h.mu.RLock()
	stored, exists := h.subscribers[topic][callback]
	h.mu.RUnlock()
	if !exists || stored.version != version {
		return nil
	}
	return h.broker.PublishUnsubscription(ctx, websubhub.VerifiedUnsubscription{
		Unsubscription: websubhub.Unsubscription{
			Mode:     websubhub.ModeUnsubscribe,
			Topic:    topic,
			Callback: callback,
		},
	})
}

func (h *kafkaHub) activeSubscriptions(topic string, now time.Time) []storedSubscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	byCallback := h.subscribers[topic]
	active := make([]storedSubscription, 0, len(byCallback))
	for callback, stored := range byCallback {
		if !stored.expiresAt.After(now) {
			delete(byCallback, callback)
			continue
		}
		active = append(active, stored)
	}
	if len(byCallback) == 0 {
		delete(h.subscribers, topic)
	}
	return active
}

func (h *kafkaHub) hasTopic(topic string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, exists := h.topics[topic]
	return exists
}

func (h *kafkaHub) hasSubscription(topic, callback string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	stored, exists := h.subscribers[topic][callback]
	return exists && stored.expiresAt.After(time.Now())
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *kafkaHub) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		h.cancel()
		h.broker.Close()
	})
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
