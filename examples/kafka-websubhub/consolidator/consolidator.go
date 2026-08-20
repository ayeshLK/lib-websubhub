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

// Package consolidator maintains the Kafka example's materialized system state
// and exposes snapshots to hub processes.
package consolidator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
)

// EventHandler applies one Kafka record at its partition offset.
type EventHandler func(context.Context, int64, []byte) error

// Broker is the consolidator's Kafka persistence boundary.
type Broker interface {
	LoadSnapshot(context.Context) (state.Snapshot, error)
	PublishSnapshot(context.Context, state.Snapshot) error
	ReplayEvents(context.Context, EventHandler) error
	ConsumeEvents(context.Context, EventHandler) error
	Close()
}

// Options configures a consolidator.
type Options struct {
	Broker Broker
	Logger *slog.Logger
}

// Consolidator owns the materialized state derived from websub-events.
type Consolidator struct {
	operationMu   sync.Mutex
	mu            sync.RWMutex
	topics        map[string]websubhub.TopicRegistration
	subscriptions map[string]state.Subscription
	revision      uint64

	broker Broker
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error

	closeOnce sync.Once
}

// New restores the latest persisted snapshot, replays retained state events,
// and then tails new events.
func New(startupContext context.Context, options Options) (*Consolidator, error) {
	if options.Broker == nil {
		return nil, errors.New("message broker is required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	snapshot, err := options.Broker.LoadSnapshot(startupContext)
	if err != nil {
		options.Broker.Close()
		return nil, fmt.Errorf("load consolidated snapshot: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consolidator := &Consolidator{
		topics:        make(map[string]websubhub.TopicRegistration),
		subscriptions: make(map[string]state.Subscription),
		revision:      snapshot.Revision,
		broker:        options.Broker,
		logger:        options.Logger,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		errors:        make(chan error, 1),
	}
	if err := consolidator.restore(startupContext, snapshot); err != nil {
		cancel()
		options.Broker.Close()
		return nil, fmt.Errorf("restore consolidated snapshot: %w", err)
	}
	if err := consolidator.broker.ReplayEvents(startupContext, consolidator.consumeEvent); err != nil {
		cancel()
		options.Broker.Close()
		return nil, fmt.Errorf("replay state events: %w", err)
	}

	go consolidator.run()
	return consolidator, nil
}

func (c *Consolidator) restore(ctx context.Context, snapshot state.Snapshot) error {
	for _, registration := range snapshot.Topics {
		if err := c.ApplyTopicRegistration(ctx, registration); err != nil {
			return err
		}
	}
	for _, subscription := range snapshot.Subscriptions {
		status := subscription.Status
		subscription.Status = ""
		if err := c.ApplySubscription(ctx, subscription); err != nil {
			return err
		}
		if status != "" {
			subscription.Status = status
			if err := c.ApplyStaleSubscription(ctx, subscription); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consolidator) run() {
	defer close(c.done)
	err := c.broker.ConsumeEvents(c.ctx, c.consumeEvent)
	if err != nil && c.ctx.Err() == nil {
		select {
		case c.errors <- fmt.Errorf("consume state events: %w", err):
		default:
		}
		c.cancel()
	}
}

func (c *Consolidator) consumeEvent(ctx context.Context, offset int64, payload []byte) error {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if err := state.ApplyEvent(ctx, payload, c); err != nil {
		if c.revision == ^uint64(0) {
			return errors.New("state snapshot revision exhausted")
		}
		return fmt.Errorf("apply state event at offset %d: %w", offset, err)
	}
	c.revision++
	c.mu.Lock()
	snapshot := c.snapshotLocked()
	c.mu.Unlock()
	if err := c.broker.PublishSnapshot(ctx, snapshot); err != nil {
		return fmt.Errorf("persist snapshot at event offset %d: %w", offset, err)
	}
	c.logger.Info("state event consolidated", "offset", offset)
	return nil
}

// Snapshot returns a detached, deterministic view of current system state.
func (c *Consolidator) Snapshot() state.Snapshot {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked()
}

func (c *Consolidator) snapshotLocked() state.Snapshot {
	snapshot := state.Snapshot{Revision: c.revision}
	for _, registration := range c.topics {
		snapshot.Topics = append(snapshot.Topics, registration)
	}
	for _, subscription := range c.subscriptions {
		snapshot.Subscriptions = append(snapshot.Subscriptions, subscription)
	}
	sort.Slice(snapshot.Topics, func(i, j int) bool {
		return snapshot.Topics[i].Topic < snapshot.Topics[j].Topic
	})
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		left, right := snapshot.Subscriptions[i], snapshot.Subscriptions[j]
		if left.Topic == right.Topic {
			return left.Callback < right.Callback
		}
		return left.Topic < right.Topic
	})
	return snapshot
}

// Handler returns the consolidator's snapshot and health HTTP endpoints.
func (c *Consolidator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/state-snapshot", c.serveSnapshot)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (c *Consolidator) serveSnapshot(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(c.Snapshot()); err != nil {
		c.logger.Error("encode state snapshot", "error", err)
	}
}

// Errors reports fatal asynchronous consolidation failures.
func (c *Consolidator) Errors() <-chan error {
	return c.errors
}

// Close stops event consumption and closes the consolidator's broker.
func (c *Consolidator) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.broker.Close()
	})
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ApplyTopicRegistration applies a valid topic-registration event.
func (c *Consolidator) ApplyTopicRegistration(_ context.Context, registration websubhub.TopicRegistration) error {
	if registration.Mode != websubhub.ModeRegister || registration.Topic == "" {
		return errors.New("invalid topic registration event")
	}
	c.mu.Lock()
	c.topics[registration.Topic] = registration
	c.mu.Unlock()
	return nil
}

// ApplyTopicDeregistration applies a valid topic-deregistration event.
func (c *Consolidator) ApplyTopicDeregistration(_ context.Context, deregistration websubhub.TopicDeregistration) error {
	if deregistration.Mode != websubhub.ModeDeregister || deregistration.Topic == "" {
		return errors.New("invalid topic deregistration event")
	}
	c.mu.Lock()
	delete(c.topics, deregistration.Topic)
	for key, subscription := range c.subscriptions {
		if subscription.Topic == deregistration.Topic {
			delete(c.subscriptions, key)
		}
	}
	c.mu.Unlock()
	return nil
}

// ApplySubscription applies a valid verified-subscription event.
func (c *Consolidator) ApplySubscription(_ context.Context, subscription state.Subscription) error {
	verified := subscription.VerifiedSubscription
	leaseSeconds, err := strconv.ParseInt(verified.EffectiveLeaseSeconds, 10, 64)
	if err != nil || leaseSeconds <= 0 || verified.Mode != websubhub.ModeSubscribe || verified.Topic == "" || verified.Callback == "" || verified.Hub == "" || verified.LeaseStartedAt.IsZero() || subscription.ServerID == "" || subscription.Status != "" {
		return errors.New("invalid verified subscription event")
	}
	key := subscriptionKey(verified.Topic, verified.Callback)
	c.mu.Lock()
	existing, found := c.subscriptions[key]
	if found && existing.Status != state.SubscriptionStatusStale {
		c.mu.Unlock()
		return nil
	}
	if found {
		subscription = existing
		subscription.Status = ""
	}
	c.subscriptions[key] = subscription
	c.mu.Unlock()
	return nil
}

// ApplyStaleSubscription applies application-owned stale delivery state.
func (c *Consolidator) ApplyStaleSubscription(_ context.Context, subscription state.Subscription) error {
	if subscription.Status != state.SubscriptionStatusStale || subscription.Mode != websubhub.ModeSubscribe || subscription.Topic == "" || subscription.Callback == "" || subscription.ServerID == "" {
		return errors.New("invalid stale subscription event")
	}
	key := subscriptionKey(subscription.Topic, subscription.Callback)
	c.mu.Lock()
	existing, found := c.subscriptions[key]
	if found && existing.ServerID == subscription.ServerID && existing.LeaseStartedAt.Equal(subscription.LeaseStartedAt) {
		existing.Status = state.SubscriptionStatusStale
		c.subscriptions[key] = existing
	}
	c.mu.Unlock()
	return nil
}

// ApplyUnsubscription applies a valid verified-unsubscription event.
func (c *Consolidator) ApplyUnsubscription(_ context.Context, verified websubhub.VerifiedUnsubscription) error {
	if verified.Mode != websubhub.ModeUnsubscribe || verified.Topic == "" || verified.Callback == "" {
		return errors.New("invalid verified unsubscription event")
	}
	c.mu.Lock()
	delete(c.subscriptions, subscriptionKey(verified.Topic, verified.Callback))
	c.mu.Unlock()
	return nil
}

func subscriptionKey(topic, callback string) string {
	return topic + "\x00" + callback
}
