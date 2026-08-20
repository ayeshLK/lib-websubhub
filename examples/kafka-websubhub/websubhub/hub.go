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

package websubhub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
)

const (
	defaultDeliveryAttempts = 3
	defaultRetryBackoff     = time.Second
	defaultMaxTopicBody     = 1 << 20 // 1 MiB
)

var errSubscriptionGone = errors.New("subscription callback is gone")

// Options configures the Kafka-backed WebSub hub application.
type Options struct {
	HubURL           string
	ServerID         string
	Broker           Broker
	SnapshotSource   SnapshotSource
	DeliveryAttempts int
	RetryBackoff     time.Duration
	MaxTopicBody     int64
	HTTPClient       *http.Client
	Logger           *slog.Logger
}

type storedSubscription struct {
	verified websubhub.VerifiedSubscription
	serverID string
	status   state.SubscriptionStatus
	groupID  string
	cancel   context.CancelFunc
}

// Hub is application code: it owns Kafka persistence, per-subscription
// delivery workers, retry policy, and application state.
type Hub struct {
	operationMu sync.Mutex
	mu          sync.RWMutex
	topics      map[string]struct{}
	subscribers map[string]map[string]*storedSubscription
	replaying   bool

	serverID         string
	broker           Broker
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

// New initializes the hub from a snapshot, replays the retained state-event
// log through a captured end boundary, and then tails new events.
func New(startupContext context.Context, options Options) (*Hub, error) {
	if options.HubURL == "" {
		return nil, errors.New("hub URL is required")
	}
	if options.ServerID == "" {
		return nil, errors.New("server ID is required")
	}
	if options.Broker == nil {
		return nil, errors.New("message broker is required")
	}
	if options.SnapshotSource == nil {
		return nil, errors.New("snapshot source is required")
	}
	if options.DeliveryAttempts <= 0 {
		options.DeliveryAttempts = defaultDeliveryAttempts
	}
	if options.RetryBackoff < 0 {
		return nil, errors.New("retry backoff must not be negative")
	}
	if options.RetryBackoff == 0 {
		options.RetryBackoff = defaultRetryBackoff
	}
	if options.MaxTopicBody <= 0 {
		options.MaxTopicBody = defaultMaxTopicBody
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	hub := &Hub{
		topics:           make(map[string]struct{}),
		subscribers:      make(map[string]map[string]*storedSubscription),
		replaying:        true,
		serverID:         options.ServerID,
		broker:           options.Broker,
		deliveryConfig:   websubhub.DeliveryConfig{HTTPClient: options.HTTPClient, Timeout: 15 * time.Second},
		deliveryAttempts: options.DeliveryAttempts,
		retryBackoff:     options.RetryBackoff,
		maxTopicBody:     options.MaxTopicBody,
		topicHTTPClient:  options.HTTPClient,
		logger:           options.Logger,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		errors:           make(chan error, 1),
	}
	snapshot, err := options.SnapshotSource.Snapshot(startupContext)
	if err != nil {
		cancel()
		hub.broker.Close()
		return nil, fmt.Errorf("retrieve initial state snapshot: %w", err)
	}
	if err := hub.applySnapshot(startupContext, snapshot); err != nil {
		cancel()
		hub.broker.Close()
		return nil, fmt.Errorf("apply initial state snapshot: %w", err)
	}
	if err := hub.broker.ReplayEvents(startupContext, hub); err != nil {
		cancel()
		hub.broker.Close()
		return nil, fmt.Errorf("replay state events: %w", err)
	}

	hub.mu.Lock()
	hub.replaying = false
	hub.mu.Unlock()

	hub.workers.Add(1)
	go hub.runEventWorker()
	hub.startReplayedSubscriptionWorkers()
	go func() {
		hub.workers.Wait()
		close(hub.done)
	}()
	return hub, nil
}

// Service returns the callback set mounted by the core WebSub handler.
func (h *Hub) Service() websubhub.Service {
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

func (h *Hub) runEventWorker() {
	defer h.workers.Done()
	if err := h.broker.ConsumeEvents(h.ctx, h); err != nil && h.ctx.Err() == nil {
		h.reportError(fmt.Errorf("event consumer: %w", err))
		h.cancel()
	}
}

func (h *Hub) reportError(err error) {
	select {
	case h.errors <- err:
	default:
	}
}

// Errors reports fatal asynchronous worker failures.
func (h *Hub) Errors() <-chan error {
	return h.errors
}

func (h *Hub) applySnapshot(ctx context.Context, snapshot state.Snapshot) error {
	for _, registration := range snapshot.Topics {
		if err := h.ApplyTopicRegistration(ctx, registration); err != nil {
			return err
		}
	}
	for _, subscription := range snapshot.Subscriptions {
		status := subscription.Status
		subscription.Status = ""
		if err := h.ApplySubscription(ctx, subscription); err != nil {
			return err
		}
		if status != "" {
			subscription.Status = status
			if err := h.ApplyStaleSubscription(ctx, subscription); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Hub) onRegisterTopic(ctx context.Context, registration websubhub.TopicRegistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if h.hasTopic(registration.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is already registered"}
	}
	if err := h.broker.PublishTopicRegistration(ctx, registration); err != nil {
		return websubhub.Result{}, err
	}
	h.logger.Info("topic registration event published", "topic", registration.Topic, "kafka_topic", kafkaTopicName(registration.Topic))
	return websubhub.Result{}, nil
}

func (h *Hub) onDeregisterTopic(ctx context.Context, deregistration websubhub.TopicDeregistration, _ websubhub.RequestMetadata) (websubhub.Result, error) {
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

func (h *Hub) onUpdateMessage(ctx context.Context, update websubhub.UpdateMessage, _ websubhub.RequestMetadata) (websubhub.Result, error) {
	if !h.hasTopic(update.Topic) {
		return websubhub.Result{}, &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	content, err := h.materializeContent(ctx, update)
	if err != nil {
		return websubhub.Result{}, err
	}
	if err := h.broker.PublishContent(ctx, kafkaTopicName(update.Topic), content); err != nil {
		return websubhub.Result{}, err
	}
	return websubhub.Result{}, nil
}

func (h *Hub) onSubscriptionValidation(_ context.Context, subscription websubhub.Subscription, _ websubhub.RequestMetadata) error {
	if !h.hasTopic(subscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	h.mu.RLock()
	stored := h.subscribers[subscription.Topic][subscription.Callback]
	status := state.SubscriptionStatus("")
	if stored != nil {
		status = stored.status
	}
	h.mu.RUnlock()
	if stored != nil && status != state.SubscriptionStatusStale {
		return &websubhub.DeniedError{Reason: "subscriber is already registered"}
	}
	return nil
}

func (h *Hub) onSubscriptionVerified(ctx context.Context, verified websubhub.VerifiedSubscription, _ websubhub.RequestMetadata) error {
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 {
		return fmt.Errorf("invalid effective lease %q", verified.EffectiveLeaseSeconds)
	}

	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if !h.hasTopic(verified.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}

	h.mu.RLock()
	existing := h.subscribers[verified.Topic][verified.Callback]
	var existingVerified websubhub.VerifiedSubscription
	serverID := h.serverID
	status := state.SubscriptionStatus("")
	if existing != nil {
		existingVerified = existing.verified
		serverID = existing.serverID
		status = existing.status
	}
	h.mu.RUnlock()
	if existing != nil {
		if status != state.SubscriptionStatusStale {
			return &websubhub.DeniedError{Reason: "subscriber is already registered"}
		}
		verified = existingVerified
	}
	subscription := state.NewSubscription(verified, serverID)
	if err := h.broker.PublishSubscription(ctx, subscription); err != nil {
		return err
	}
	h.logger.Info("subscription event published", "topic", verified.Topic, "server_id", serverID, "consumer_group", subscriptionGroupName(verified))
	return nil
}

func (h *Hub) onUnsubscriptionValidation(_ context.Context, unsubscription websubhub.Unsubscription, _ websubhub.RequestMetadata) error {
	if !h.hasTopic(unsubscription.Topic) {
		return &websubhub.DeniedError{Reason: "topic is not registered"}
	}
	if !h.hasSubscription(unsubscription.Topic, unsubscription.Callback) {
		return &websubhub.DeniedError{Reason: "subscription does not exist"}
	}
	return nil
}

func (h *Hub) onUnsubscriptionVerified(ctx context.Context, verified websubhub.VerifiedUnsubscription, _ websubhub.RequestMetadata) error {
	h.operationMu.Lock()
	defer h.operationMu.Unlock()
	if err := h.broker.PublishUnsubscription(ctx, verified); err != nil {
		return err
	}
	h.logger.Info("unsubscription event published", "topic", verified.Topic)
	return nil
}

func (h *Hub) ApplyTopicRegistration(_ context.Context, registration websubhub.TopicRegistration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if registration.Mode != websubhub.ModeRegister || registration.Topic == "" {
		return errors.New("invalid topic registration event")
	}
	h.topics[registration.Topic] = struct{}{}
	h.logger.Info("state event applied", "mode", registration.Mode, "topic", registration.Topic)
	return nil
}

func (h *Hub) ApplyTopicDeregistration(_ context.Context, deregistration websubhub.TopicDeregistration) error {
	if deregistration.Mode != websubhub.ModeDeregister || deregistration.Topic == "" {
		return errors.New("invalid topic deregistration event")
	}
	h.mu.Lock()
	byCallback := h.subscribers[deregistration.Topic]
	delete(h.topics, deregistration.Topic)
	delete(h.subscribers, deregistration.Topic)
	h.mu.Unlock()
	for _, stored := range byCallback {
		if stored.cancel != nil {
			stored.cancel()
		}
	}
	h.logger.Info("state event applied", "mode", deregistration.Mode, "topic", deregistration.Topic)
	return nil
}

func (h *Hub) ApplySubscription(_ context.Context, subscription state.Subscription) error {
	verified := subscription.VerifiedSubscription
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 || verified.Mode != websubhub.ModeSubscribe || verified.Topic == "" || verified.Callback == "" || verified.Hub == "" || verified.LeaseStartedAt.IsZero() || subscription.ServerID == "" || subscription.Status != "" {
		return errors.New("invalid verified subscription event")
	}

	h.mu.Lock()
	byCallback := h.subscribers[verified.Topic]
	if byCallback == nil {
		byCallback = make(map[string]*storedSubscription)
		h.subscribers[verified.Topic] = byCallback
	}
	existing := byCallback[verified.Callback]
	if existing != nil && existing.status != state.SubscriptionStatusStale {
		h.mu.Unlock()
		return nil
	}
	if existing != nil {
		verified = existing.verified
		subscription.ServerID = existing.serverID
	}
	stored := &storedSubscription{
		verified: verified,
		serverID: subscription.ServerID,
		groupID:  subscriptionGroupName(verified),
	}
	byCallback[verified.Callback] = stored
	startWorker := !h.replaying
	h.mu.Unlock()

	if startWorker {
		h.startSubscriptionWorker(stored)
	}
	h.logger.Info("state event applied", "mode", verified.Mode, "topic", verified.Topic, "server_id", subscription.ServerID)
	return nil
}

func (h *Hub) ApplyStaleSubscription(_ context.Context, stale state.Subscription) error {
	if stale.Status != state.SubscriptionStatusStale || stale.Mode != websubhub.ModeSubscribe || stale.Topic == "" || stale.Callback == "" || stale.ServerID == "" {
		return errors.New("invalid stale subscription event")
	}
	h.mu.Lock()
	stored := h.subscribers[stale.Topic][stale.Callback]
	if stored == nil || stored.serverID != stale.ServerID || !stored.verified.LeaseStartedAt.Equal(stale.LeaseStartedAt) {
		h.mu.Unlock()
		return nil
	}
	stored.status = state.SubscriptionStatusStale
	cancel := stored.cancel
	stored.cancel = nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	h.logger.Info("stale subscription event applied", "topic", stale.Topic)
	return nil
}

func (h *Hub) ApplyUnsubscription(_ context.Context, verified websubhub.VerifiedUnsubscription) error {
	if verified.Mode != websubhub.ModeUnsubscribe || verified.Topic == "" || verified.Callback == "" {
		return errors.New("invalid verified unsubscription event")
	}
	h.mu.Lock()
	byCallback := h.subscribers[verified.Topic]
	stored := byCallback[verified.Callback]
	delete(byCallback, verified.Callback)
	if len(byCallback) == 0 {
		delete(h.subscribers, verified.Topic)
	}
	h.mu.Unlock()
	if stored != nil && stored.cancel != nil {
		stored.cancel()
	}
	h.logger.Info("state event applied", "mode", verified.Mode, "topic", verified.Topic)
	return nil
}

func (h *Hub) startReplayedSubscriptionWorkers() {
	h.mu.RLock()
	var subscriptions []*storedSubscription
	for _, byCallback := range h.subscribers {
		for _, stored := range byCallback {
			if stored.status != state.SubscriptionStatusStale {
				subscriptions = append(subscriptions, stored)
			}
		}
	}
	h.mu.RUnlock()
	for _, stored := range subscriptions {
		h.startSubscriptionWorker(stored)
	}
}

func (h *Hub) startSubscriptionWorker(stored *storedSubscription) {
	h.mu.Lock()
	current := h.subscribers[stored.verified.Topic][stored.verified.Callback]
	if current != stored || stored.serverID != h.serverID || stored.status == state.SubscriptionStatusStale || stored.cancel != nil || h.ctx.Err() != nil {
		h.mu.Unlock()
		return
	}
	workerContext, cancel := context.WithCancel(h.ctx)
	stored.cancel = cancel
	verified := stored.verified
	groupID := stored.groupID
	h.workers.Add(1)
	h.mu.Unlock()

	go func() {
		defer h.workers.Done()
		err := h.broker.ConsumeSubscription(
			workerContext,
			kafkaTopicName(verified.Topic),
			groupID,
			func(ctx context.Context, messages []ContentMessage) error {
				return h.deliverBatch(ctx, verified, messages)
			},
		)
		if workerContext.Err() != nil || errors.Is(err, errSubscriptionGone) {
			return
		}
		if err == nil {
			return
		}
		stale := state.NewStaleSubscription(verified, stored.serverID)
		if publishErr := h.broker.PublishStaleSubscription(h.ctx, stale); publishErr != nil {
			h.reportError(fmt.Errorf("persist stale subscription: %w", publishErr))
			return
		}
		h.logger.Error("subscription worker marked stale", "topic", verified.Topic, "error", err)
	}()
}

func (h *Hub) materializeContent(ctx context.Context, update websubhub.UpdateMessage) (ContentMessage, error) {
	if update.Kind == websubhub.UpdateContent {
		content := ContentMessage{ContentType: update.ContentType, Body: append([]byte(nil), update.Body...)}
		if err := validateJSONContent(content); err != nil {
			return ContentMessage{}, err
		}
		return content, nil
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, update.Topic, nil)
	if err != nil {
		return ContentMessage{}, err
	}
	response, err := h.topicHTTPClient.Do(request)
	if err != nil {
		return ContentMessage{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ContentMessage{}, fmt.Errorf("topic returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, h.maxTopicBody+1))
	if err != nil {
		return ContentMessage{}, err
	}
	if int64(len(body)) > h.maxTopicBody {
		return ContentMessage{}, fmt.Errorf("topic response exceeds %d bytes", h.maxTopicBody)
	}
	content := ContentMessage{
		ContentType: response.Header.Get("Content-Type"),
		Body:        body,
	}
	if err := validateJSONContent(content); err != nil {
		return ContentMessage{}, err
	}
	return content, nil
}

func validateJSONContent(content ContentMessage) error {
	mediaType, _, err := mime.ParseMediaType(content.ContentType)
	if err != nil || mediaType != "application/json" {
		return errors.New("content must have the application/json media type")
	}
	if !json.Valid(content.Body) {
		return errors.New("content must contain valid JSON")
	}
	return nil
}

func (h *Hub) deliverBatch(ctx context.Context, verified websubhub.VerifiedSubscription, messages []ContentMessage) error {
	client, err := websubhub.NewDeliveryClient(verified.Subscription, h.deliveryConfig)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := h.deliverContent(ctx, client, verified, message); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) deliverContent(ctx context.Context, client *websubhub.DeliveryClient, verified websubhub.VerifiedSubscription, message ContentMessage) error {
	var deliveryErr error
	for attempt := 1; attempt <= h.deliveryAttempts; attempt++ {
		_, deliveryErr = client.Deliver(ctx, websubhub.ContentDistribution{
			ContentType: message.ContentType,
			Body:        append([]byte(nil), message.Body...),
		})
		if deliveryErr == nil {
			return nil
		}
		if errors.Is(deliveryErr, websubhub.ErrSubscriptionGone) {
			if err := h.broker.PublishUnsubscription(ctx, websubhub.VerifiedUnsubscription{
				Unsubscription: websubhub.Unsubscription{
					Mode:     websubhub.ModeUnsubscribe,
					Topic:    verified.Topic,
					Callback: verified.Callback,
				},
			}); err != nil {
				return err
			}
			return errSubscriptionGone
		}
		if attempt < h.deliveryAttempts {
			if err := waitContext(ctx, h.retryBackoff); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("content delivery failed after %d attempts: %w", h.deliveryAttempts, deliveryErr)
}

func kafkaTopicName(topic string) string {
	return "websub-topic-" + hashIdentifier(topic)
}

func subscriptionGroupName(verified websubhub.VerifiedSubscription) string {
	key := verified.Topic + "\x00" + verified.Callback + "\x00" + verified.LeaseStartedAt.UTC().Format(time.RFC3339Nano)
	return "websub-subscriber-" + hashIdentifier(key)
}

func hashIdentifier(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (h *Hub) hasTopic(topic string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, exists := h.topics[topic]
	return exists
}

func (h *Hub) hasSubscription(topic, callback string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.subscribers[topic][callback] != nil
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

func (h *Hub) Close(ctx context.Context) error {
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
