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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

type queuedUpdate struct {
	id     string
	update updateEvent
}

type fakeBroker struct {
	mu                sync.Mutex
	seed              []stateEvent
	state             []stateEvent
	events            chan stateEvent
	appliedEvents     chan stateEvent
	eventConsumerGate <-chan struct{}
	updates           chan queuedUpdate
	published         chan updateEvent
	dead              chan deadLetter
	nextID            atomic.Uint64
}

func newFakeBroker(seed ...stateEvent) *fakeBroker {
	return &fakeBroker{
		seed:          append([]stateEvent(nil), seed...),
		events:        make(chan stateEvent, 16),
		appliedEvents: make(chan stateEvent, 16),
		updates:       make(chan queuedUpdate, 16),
		published:     make(chan updateEvent, 16),
		dead:          make(chan deadLetter, 16),
	}
}

func newPausedFakeBroker() (*fakeBroker, chan struct{}) {
	gate := make(chan struct{})
	broker := newFakeBroker()
	broker.eventConsumerGate = gate
	return broker, gate
}

func (b *fakeBroker) PublishEvent(ctx context.Context, event stateEvent) error {
	b.mu.Lock()
	b.state = append(b.state, event)
	b.mu.Unlock()
	select {
	case b.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBroker) PublishUpdate(ctx context.Context, update updateEvent) error {
	cloned := updateEvent{Topic: update.Topic, ContentType: update.ContentType, Body: append([]byte(nil), update.Body...)}
	queued := queuedUpdate{id: "updates-0-" + formatUint(b.nextID.Add(1)-1), update: cloned}
	select {
	case b.published <- cloned:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case b.updates <- queued:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBroker) PublishDeadLetter(ctx context.Context, letter deadLetter) error {
	select {
	case b.dead <- letter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBroker) ReplayEvents(ctx context.Context, apply eventConsumer) error {
	for _, event := range b.seed {
		if err := apply(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (b *fakeBroker) ConsumeEvents(ctx context.Context, apply eventConsumer) error {
	if b.eventConsumerGate != nil {
		select {
		case <-b.eventConsumerGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		select {
		case event := <-b.events:
			if err := apply(ctx, event); err != nil {
				return err
			}
			select {
			case b.appliedEvents <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *fakeBroker) ConsumeUpdates(ctx context.Context, consume updateConsumer) error {
	for {
		select {
		case queued := <-b.updates:
			if err := consume(ctx, queued.id, queued.update); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *fakeBroker) Close() {}

func (b *fakeBroker) publishedStateEvents() []stateEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]stateEvent(nil), b.state...)
}

func waitForAppliedEvent(t *testing.T, broker *fakeBroker, want websubhub.Mode) {
	t.Helper()
	select {
	case event := <-broker.appliedEvents:
		if event.Mode != want {
			t.Fatalf("applied event mode = %q, want %q", event.Mode, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %q event application", want)
	}
}

func TestPublishedEventIsAppliedOnlyByConsumer(t *testing.T) {
	broker, gate := newPausedFakeBroker()
	hub := newTestHub(t, broker)
	topic := "http://publisher.example/topics/eventual"

	if _, err := hub.onRegisterTopic(context.Background(), websubhub.TopicRegistration{
		Mode:  websubhub.ModeRegister,
		Topic: topic,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish registration event: %v", err)
	}
	if hub.hasTopic(topic) {
		t.Fatal("producer updated in-memory state before the event consumer")
	}
	events := broker.publishedStateEvents()
	if len(events) != 1 || events[0].Mode != websubhub.ModeRegister {
		t.Fatalf("published events = %+v", events)
	}

	close(gate)
	waitForAppliedEvent(t, broker, websubhub.ModeRegister)
	if !hub.hasTopic(topic) {
		t.Fatal("event consumer did not update in-memory state")
	}
}

func TestStateReplayAndKafkaDelivery(t *testing.T) {
	requests := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read delivery: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		requests <- request.Clone(context.Background())
		bodies <- body
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer callback.Close()

	topic := "http://publisher.example/topics/orders"
	secret := "broker-secret"
	broker := newFakeBroker(
		stateEvent{Mode: websubhub.ModeRegister, Topic: topic},
		stateEvent{Mode: websubhub.ModeSubscribe, Topic: topic, Subscription: &persistedSubscription{
			Hub:       "http://hub.example/hub",
			Topic:     topic,
			Callback:  callback.URL,
			Secret:    secret,
			ExpiresAt: time.Now().Add(time.Hour),
		}},
	)
	hub := newTestHub(t, broker)
	payload := []byte(`{"order":"A-42"}`)
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: "application/json; charset=utf-8",
		Body:        payload,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish update: %v", err)
	}

	select {
	case request := <-requests:
		body := <-bodies
		if string(body) != string(payload) {
			t.Errorf("delivery body = %q, want %q", body, payload)
		}
		if got := request.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := request.Header.Get("X-WebSubHub-Message-ID"); got != "" {
			t.Errorf("non-standard message ID header = %q", got)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(payload)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := request.Header.Get(websubhub.HeaderHubSignature); got != wantSignature {
			t.Errorf("signature = %q, want %q", got, wantSignature)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Kafka-backed delivery")
	}
}

func TestStateChangesArePublishedAndApplied(t *testing.T) {
	broker := newFakeBroker()
	hub := newTestHub(t, broker)
	topic := "http://publisher.example/topics/orders"
	callback := "http://subscriber.example/callback"

	if _, err := hub.onRegisterTopic(context.Background(), websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("register topic: %v", err)
	}
	waitForAppliedEvent(t, broker, websubhub.ModeRegister)
	if err := hub.onSubscriptionVerified(context.Background(), websubhub.VerifiedSubscription{
		Subscription: websubhub.Subscription{
			Hub:      "http://hub.example/hub",
			Mode:     websubhub.ModeSubscribe,
			Topic:    topic,
			Callback: callback,
			Secret:   "secret",
		},
		EffectiveLeaseSeconds: "300",
		LeaseStartedAt:        time.Now(),
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish subscription event: %v", err)
	}
	waitForAppliedEvent(t, broker, websubhub.ModeSubscribe)
	if !hub.hasSubscription(topic, callback) {
		t.Fatal("verified subscription was not applied")
	}
	if err := hub.onUnsubscriptionVerified(context.Background(), websubhub.VerifiedUnsubscription{
		Unsubscription: websubhub.Unsubscription{Mode: websubhub.ModeUnsubscribe, Topic: topic, Callback: callback},
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish unsubscription event: %v", err)
	}
	waitForAppliedEvent(t, broker, websubhub.ModeUnsubscribe)
	if hub.hasSubscription(topic, callback) {
		t.Fatal("unsubscription event was not applied")
	}
	if _, err := hub.onDeregisterTopic(context.Background(), websubhub.TopicDeregistration{Mode: websubhub.ModeDeregister, Topic: topic}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("deregister topic: %v", err)
	}
	waitForAppliedEvent(t, broker, websubhub.ModeDeregister)
	if hub.hasTopic(topic) {
		t.Fatal("deregistration event was not applied")
	}

	events := broker.publishedStateEvents()
	wantModes := []websubhub.Mode{websubhub.ModeRegister, websubhub.ModeSubscribe, websubhub.ModeUnsubscribe, websubhub.ModeDeregister}
	if len(events) != len(wantModes) {
		t.Fatalf("published event count = %d, want %d", len(events), len(wantModes))
	}
	for index, want := range wantModes {
		if events[index].Mode != want {
			t.Errorf("published event %d mode = %q, want %q", index, events[index].Mode, want)
		}
	}
}

func TestDeliveryExhaustionPublishesDeadLetter(t *testing.T) {
	var attempts atomic.Int32
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer callback.Close()

	topic := "http://publisher.example/topics/retry"
	broker := newFakeBroker(
		stateEvent{Mode: websubhub.ModeRegister, Topic: topic},
		stateEvent{Mode: websubhub.ModeSubscribe, Topic: topic, Subscription: &persistedSubscription{
			Hub:       "http://hub.example/hub",
			Topic:     topic,
			Callback:  callback.URL,
			ExpiresAt: time.Now().Add(time.Hour),
		}},
	)
	hub := newTestHub(t, broker)
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: "text/plain",
		Body:        []byte("retry me"),
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish update: %v", err)
	}

	select {
	case letter := <-broker.dead:
		if letter.Attempts != 2 {
			t.Errorf("dead-letter attempts = %d, want 2", letter.Attempts)
		}
		if got := attempts.Load(); got != 2 {
			t.Errorf("delivery attempts = %d, want 2", got)
		}
		if letter.RecordID != "updates-0-0" || string(letter.Update.Body) != "retry me" {
			t.Errorf("dead letter = %+v", letter)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for dead letter")
	}
}

func TestEventNotificationIsMaterializedBeforeKafka(t *testing.T) {
	topicServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
		_, _ = io.WriteString(writer, "<feed>current</feed>")
	}))
	defer topicServer.Close()

	broker := newFakeBroker(stateEvent{Mode: websubhub.ModeRegister, Topic: topicServer.URL})
	hub := newTestHub(t, broker)
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:  websubhub.UpdateEvent,
		Topic: topicServer.URL,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish event notification: %v", err)
	}

	select {
	case update := <-broker.published:
		if update.ContentType != "application/atom+xml; charset=utf-8" || string(update.Body) != "<feed>current</feed>" {
			t.Errorf("materialized update = %+v", update)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for materialized Kafka update")
	}
}

func newTestHub(t *testing.T, broker *fakeBroker) *kafkaHub {
	t.Helper()
	hub, err := newKafkaHub(context.Background(), hubOptions{
		hubURL:           "http://hub.example/hub",
		broker:           broker,
		deliveryAttempts: 2,
		retryBackoff:     time.Nanosecond,
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new Kafka hub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := hub.Close(ctx); err != nil {
			t.Errorf("close Kafka hub: %v", err)
		}
	})
	return hub
}

func formatUint(value uint64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = digits[value%10]
		value /= 10
	}
	return string(buffer[position:])
}
