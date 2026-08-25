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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
)

const testServerID = "server-1"

type fakeConsumer struct {
	topic string
	batch chan []ContentMessage
}

type consumerIdentity struct {
	topic string
	group string
}

type publishedContent struct {
	topic   string
	content ContentMessage
}

type fakeBroker struct {
	mu                sync.Mutex
	seed              []any
	state             []any
	events            chan any
	appliedEvents     chan string
	eventConsumerGate <-chan struct{}
	consumers         map[string]*fakeConsumer
	consumerStarted   chan consumerIdentity
	contentPublished  chan publishedContent
	committed         chan string
}

func newFakeBroker(seed ...any) *fakeBroker {
	normalized := make([]any, len(seed))
	for index, event := range seed {
		if verified, ok := event.(websubhub.VerifiedSubscription); ok {
			event = state.NewSubscription(verified, testServerID)
		}
		normalized[index] = event
	}
	return &fakeBroker{
		seed:             normalized,
		events:           make(chan any, 32),
		appliedEvents:    make(chan string, 32),
		consumers:        make(map[string]*fakeConsumer),
		consumerStarted:  make(chan consumerIdentity, 32),
		contentPublished: make(chan publishedContent, 32),
		committed:        make(chan string, 32),
	}
}

func newPausedFakeBroker() (*fakeBroker, chan struct{}) {
	gate := make(chan struct{})
	broker := newFakeBroker()
	broker.eventConsumerGate = gate
	return broker, gate
}

func (b *fakeBroker) PublishTopicRegistration(ctx context.Context, registration websubhub.TopicRegistration) error {
	return b.publishEvent(ctx, registration)
}

func (b *fakeBroker) PublishTopicDeregistration(ctx context.Context, deregistration websubhub.TopicDeregistration) error {
	return b.publishEvent(ctx, deregistration)
}

func (b *fakeBroker) PublishSubscription(ctx context.Context, subscription state.Subscription) error {
	return b.publishEvent(ctx, subscription)
}

func (b *fakeBroker) PublishStaleSubscription(ctx context.Context, subscription state.Subscription) error {
	return b.publishEvent(ctx, subscription)
}

func (b *fakeBroker) PublishUnsubscription(ctx context.Context, unsubscription websubhub.VerifiedUnsubscription) error {
	return b.publishEvent(ctx, unsubscription)
}

func (b *fakeBroker) publishEvent(ctx context.Context, event any) error {
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

func (b *fakeBroker) PublishContent(ctx context.Context, topic string, content ContentMessage) error {
	cloned := cloneContent(content)
	select {
	case b.contentPublished <- publishedContent{topic: topic, content: cloned}:
	case <-ctx.Done():
		return ctx.Err()
	}

	b.mu.Lock()
	var consumers []*fakeConsumer
	for _, consumer := range b.consumers {
		if consumer.topic == topic {
			consumers = append(consumers, consumer)
		}
	}
	b.mu.Unlock()
	for _, consumer := range consumers {
		select {
		case consumer.batch <- []ContentMessage{cloneContent(content)}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *fakeBroker) ReplayEvents(ctx context.Context, consumer state.Consumer) error {
	for _, event := range b.seed {
		if err := applyFakeEvent(ctx, event, consumer); err != nil {
			return err
		}
	}
	return nil
}

func (b *fakeBroker) ConsumeEvents(ctx context.Context, consumer state.Consumer) error {
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
			if err := applyFakeEvent(ctx, event, consumer); err != nil {
				return err
			}
			select {
			case b.appliedEvents <- fakeEventLabel(event):
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *fakeBroker) ConsumeSubscription(ctx context.Context, topic, group string, consume ContentBatchConsumer) error {
	consumer := &fakeConsumer{topic: topic, batch: make(chan []ContentMessage, 8)}
	b.mu.Lock()
	b.consumers[group] = consumer
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if b.consumers[group] == consumer {
			delete(b.consumers, group)
		}
		b.mu.Unlock()
	}()

	select {
	case b.consumerStarted <- consumerIdentity{topic: topic, group: group}:
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		select {
		case batch := <-consumer.batch:
			if err := consume(ctx, cloneBatch(batch)); err != nil {
				return err
			}
			select {
			case b.committed <- group:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *fakeBroker) Close() {}

func (b *fakeBroker) publishedStateEvents() []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]any(nil), b.state...)
}

func (b *fakeBroker) enqueueBatch(t *testing.T, group string, batch []ContentMessage) {
	t.Helper()
	b.mu.Lock()
	consumer := b.consumers[group]
	b.mu.Unlock()
	if consumer == nil {
		t.Fatalf("consumer group %q is not running", group)
	}
	select {
	case consumer.batch <- cloneBatch(batch):
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out enqueueing content for %q", group)
	}
}

func cloneContent(content ContentMessage) ContentMessage {
	return ContentMessage{ContentType: content.ContentType, Body: append([]byte(nil), content.Body...)}
}

func cloneBatch(batch []ContentMessage) []ContentMessage {
	cloned := make([]ContentMessage, len(batch))
	for index, content := range batch {
		cloned[index] = cloneContent(content)
	}
	return cloned
}

func applyFakeEvent(ctx context.Context, event any, consumer state.Consumer) error {
	switch event := event.(type) {
	case websubhub.TopicRegistration:
		return consumer.ApplyTopicRegistration(ctx, event)
	case websubhub.TopicDeregistration:
		return consumer.ApplyTopicDeregistration(ctx, event)
	case state.Subscription:
		if event.Status == "" {
			return consumer.ApplySubscription(ctx, event)
		}
		return consumer.ApplyStaleSubscription(ctx, event)
	case websubhub.VerifiedUnsubscription:
		return consumer.ApplyUnsubscription(ctx, event)
	default:
		return errors.New("unsupported fake event")
	}
}

func fakeEventLabel(event any) string {
	switch event := event.(type) {
	case websubhub.TopicRegistration:
		return string(event.Mode)
	case websubhub.TopicDeregistration:
		return string(event.Mode)
	case state.Subscription:
		if event.Status == "" {
			return string(event.Mode)
		}
		return string(event.Status)
	case websubhub.VerifiedUnsubscription:
		return string(event.Mode)
	default:
		return "unknown"
	}
}

func waitForAppliedEvent(t *testing.T, broker *fakeBroker, want string) {
	t.Helper()
	select {
	case event := <-broker.appliedEvents:
		if event != want {
			t.Fatalf("applied event = %q, want %q", event, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %q event application", want)
	}
}

func waitForConsumer(t *testing.T, broker *fakeBroker) consumerIdentity {
	t.Helper()
	select {
	case consumer := <-broker.consumerStarted:
		return consumer
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for subscription consumer")
		return consumerIdentity{}
	}
}

func verifiedSubscription(topic, callback string, started time.Time) websubhub.VerifiedSubscription {
	return websubhub.VerifiedSubscription{
		Subscription: websubhub.Subscription{
			Hub:      "http://hub.example/hub",
			Mode:     websubhub.ModeSubscribe,
			Topic:    topic,
			Callback: callback,
			Secret:   "broker-secret",
		},
		EffectiveLeaseSeconds: "300",
		LeaseStartedAt:        started,
	}
}

func TestPublishedStateIsAppliedOnlyByConsumer(t *testing.T) {
	broker, gate := newPausedFakeBroker()
	hub := newTestHub(t, broker)
	topic := "http://publisher.example/topics/eventual"

	if _, err := hub.onRegisterTopic(context.Background(), websubhub.TopicRegistration{
		Mode:        websubhub.ModeRegister,
		Topic:       topic,
		ContentType: "application/json; charset=utf-8",
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish registration event: %v", err)
	}
	if hub.hasTopic(topic) {
		t.Fatal("producer updated in-memory state before the event consumer")
	}

	close(gate)
	waitForAppliedEvent(t, broker, string(websubhub.ModeRegister))
	if !hub.hasTopic(topic) {
		t.Fatal("event consumer did not update in-memory state")
	}
	hub.mu.RLock()
	registration := hub.topics[topic]
	hub.mu.RUnlock()
	if registration.ContentType != "application/json; charset=utf-8" {
		t.Fatalf("registered content type = %q", registration.ContentType)
	}
}

func TestSubscriptionWorkerDeliversMappedJSONContent(t *testing.T) {
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

	topic := "http://publisher.example/topics/orders?region=west"
	started := time.Date(2026, time.August, 19, 10, 30, 0, 0, time.UTC)
	verified := verifiedSubscription(topic, callback.URL, started)
	broker := newFakeBroker(
		websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic},
		verified,
	)
	hub := newTestHub(t, broker)
	consumer := waitForConsumer(t, broker)
	if consumer.topic != kafkaTopicName(topic) {
		t.Fatalf("Kafka topic = %q, want %q", consumer.topic, kafkaTopicName(topic))
	}
	if consumer.group != subscriptionGroupName(verified) {
		t.Fatalf("consumer group = %q, want %q", consumer.group, subscriptionGroupName(verified))
	}

	payload := []byte(`{"order":"A-42"}`)
	contentType := "application/json; charset=utf-8"
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: contentType,
		Body:        payload,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish update: %v", err)
	}

	select {
	case published := <-broker.contentPublished:
		if published.topic != kafkaTopicName(topic) || string(published.content.Body) != string(payload) {
			t.Errorf("published content = %+v", published)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Kafka publication")
	}
	select {
	case request := <-requests:
		body := <-bodies
		if string(body) != string(payload) {
			t.Errorf("delivery body = %q, want %q", body, payload)
		}
		if got := request.Header.Get("Content-Type"); got != contentType {
			t.Errorf("Content-Type = %q, want %q", got, contentType)
		}
		mac := hmac.New(sha256.New, []byte(verified.Secret))
		_, _ = mac.Write(payload)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := request.Header.Get(websubhub.HeaderHubSignature); got != wantSignature {
			t.Errorf("signature = %q, want %q", got, wantSignature)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for subscriber delivery")
	}
	select {
	case group := <-broker.committed:
		if group != consumer.group {
			t.Errorf("committed group = %q, want %q", group, consumer.group)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Kafka commit")
	}
}

func TestVerifiedSubscriptionIsTaggedWithCurrentServer(t *testing.T) {
	topic := "http://publisher.example/topics/new-owner"
	broker := newFakeBroker(websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic})
	hub := newTestHub(t, broker)
	verified := verifiedSubscription(topic, "http://subscriber.example/callback", time.Now())

	if err := hub.onSubscriptionVerified(context.Background(), verified, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish subscription: %v", err)
	}
	events := broker.publishedStateEvents()
	if len(events) != 1 {
		t.Fatalf("published events = %d, want 1", len(events))
	}
	subscription, ok := events[0].(state.Subscription)
	if !ok || subscription.ServerID != testServerID {
		t.Fatalf("published subscription = %#v, want owner %q", events[0], testServerID)
	}
	waitForAppliedEvent(t, broker, string(websubhub.ModeSubscribe))
}

func TestSubscriptionWorkersStartOnlyForOwningServer(t *testing.T) {
	topic := "http://publisher.example/topics/owned"
	local := verifiedSubscription(topic, "http://subscriber.example/local", time.Now())
	foreign := verifiedSubscription(topic, "http://subscriber.example/foreign", time.Now().Add(time.Second))
	broker := newFakeBroker(
		websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic},
		state.NewSubscription(local, testServerID),
		state.NewSubscription(foreign, "server-2"),
	)
	hub := newTestHub(t, broker)

	hub.mu.RLock()
	localStored := hub.subscribers[topic][local.Callback]
	foreignStored := hub.subscribers[topic][foreign.Callback]
	hub.mu.RUnlock()
	if localStored == nil || localStored.cancel == nil {
		t.Fatal("owning server did not start its subscription worker")
	}
	if foreignStored == nil || foreignStored.serverID != "server-2" {
		t.Fatalf("foreign subscription = %+v", foreignStored)
	}
	if foreignStored.cancel != nil {
		t.Fatal("non-owning server started a foreign subscription worker")
	}

	consumer := waitForConsumer(t, broker)
	if consumer.group != subscriptionGroupName(local) {
		t.Fatalf("started consumer group = %q, want local subscription group", consumer.group)
	}
}

func TestStaleSubscriptionIsFlatAndRecoveryReusesConsumerGroup(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer callback.Close()

	topic := "http://publisher.example/topics/stale"
	started := time.Date(2026, time.August, 19, 11, 0, 0, 0, time.UTC)
	verified := verifiedSubscription(topic, callback.URL, started)
	broker := newFakeBroker(
		websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic},
		verified,
	)
	hub := newTestHub(t, broker)
	firstConsumer := waitForConsumer(t, broker)

	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: "application/json",
		Body:        []byte(`{"state":"fail"}`),
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish failing update: %v", err)
	}
	waitForAppliedEvent(t, broker, string(state.SubscriptionStatusStale))

	hub.mu.RLock()
	stored := hub.subscribers[topic][callback.URL]
	hub.mu.RUnlock()
	if stored == nil || stored.status != state.SubscriptionStatusStale {
		t.Fatalf("stored subscription = %+v, want stale", stored)
	}

	events := broker.publishedStateEvents()
	var stale state.Subscription
	for _, event := range events {
		if candidate, ok := event.(state.Subscription); ok {
			stale = candidate
		}
	}
	payload, err := json.Marshal(stale)
	if err != nil {
		t.Fatalf("encode stale subscription: %v", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatalf("inspect stale subscription JSON: %v", err)
	}
	if _, exists := object["VerifiedSubscription"]; exists {
		t.Fatalf("stale subscription is nested instead of flat: %s", payload)
	}
	if got := string(object["status"]); got != `"stale"` {
		t.Fatalf("stale status JSON = %s", got)
	}
	if got := string(object["server_id"]); got != `"server-1"` {
		t.Fatalf("server ID JSON = %s", got)
	}
	decodedHub := &Hub{
		topics:      make(map[string]websubhub.TopicRegistration),
		subscribers: make(map[string]map[string]*storedSubscription),
		replaying:   true,
		serverID:    testServerID,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := decodedHub.ApplySubscription(context.Background(), state.NewSubscription(verified, testServerID)); err != nil {
		t.Fatalf("seed decoded subscription state: %v", err)
	}
	if err := state.ApplyEvent(context.Background(), payload, decodedHub); err != nil {
		t.Fatalf("decode stale subscription event: %v", err)
	}
	if got := decodedHub.subscribers[topic][callback.URL].status; got != state.SubscriptionStatusStale {
		t.Fatalf("decoded subscription status = %q, want %q", got, state.SubscriptionStatusStale)
	}

	recovery := verifiedSubscription(topic, callback.URL, started.Add(time.Hour))
	if err := hub.onSubscriptionValidation(context.Background(), recovery.Subscription, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("validate stale subscription recovery: %v", err)
	}
	if err := hub.onSubscriptionVerified(context.Background(), recovery, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish stale subscription recovery: %v", err)
	}
	waitForAppliedEvent(t, broker, string(websubhub.ModeSubscribe))
	secondConsumer := waitForConsumer(t, broker)
	if secondConsumer.group != firstConsumer.group {
		t.Fatalf("recovery group = %q, want original %q", secondConsumer.group, firstConsumer.group)
	}
}

func TestUnsubscribeThenResubscribeUsesNewConsumerGroup(t *testing.T) {
	topic := "http://publisher.example/topics/generations"
	callback := "http://subscriber.example/callback"
	first := verifiedSubscription(topic, callback, time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC))
	broker := newFakeBroker(
		websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic},
		first,
	)
	hub := newTestHub(t, broker)
	firstConsumer := waitForConsumer(t, broker)

	if err := hub.onUnsubscriptionVerified(context.Background(), websubhub.VerifiedUnsubscription{
		Unsubscription: websubhub.Unsubscription{
			Mode:     websubhub.ModeUnsubscribe,
			Topic:    topic,
			Callback: callback,
		},
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish unsubscription: %v", err)
	}
	waitForAppliedEvent(t, broker, string(websubhub.ModeUnsubscribe))

	second := verifiedSubscription(topic, callback, first.LeaseStartedAt.Add(time.Hour))
	if err := hub.onSubscriptionVerified(context.Background(), second, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish resubscription: %v", err)
	}
	waitForAppliedEvent(t, broker, string(websubhub.ModeSubscribe))
	secondConsumer := waitForConsumer(t, broker)
	if secondConsumer.group == firstConsumer.group {
		t.Fatalf("resubscription reused consumer group %q", secondConsumer.group)
	}
}

func TestFailedBatchIsNotCommitted(t *testing.T) {
	var requests atomic.Int32
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer callback.Close()

	topic := "http://publisher.example/topics/batch"
	verified := verifiedSubscription(topic, callback.URL, time.Now())
	broker := newFakeBroker(
		websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic},
		verified,
	)
	newTestHub(t, broker)
	consumer := waitForConsumer(t, broker)
	broker.enqueueBatch(t, consumer.group, []ContentMessage{
		{ContentType: "application/json", Body: []byte(`{"sequence":1}`)},
		{ContentType: "application/json", Body: []byte(`{"sequence":2}`)},
	})
	waitForAppliedEvent(t, broker, string(state.SubscriptionStatusStale))

	select {
	case group := <-broker.committed:
		t.Fatalf("failed batch unexpectedly committed for group %q", group)
	default:
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("delivery requests = %d, want 3 (one success plus two attempts)", got)
	}
}

func TestContentMustBeJSON(t *testing.T) {
	topic := "http://publisher.example/topics/json"
	broker := newFakeBroker(websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic})
	hub := newTestHub(t, broker)

	tests := []websubhub.UpdateMessage{
		{Kind: websubhub.UpdateContent, Topic: topic, ContentType: "text/plain", Body: []byte(`{"valid":"json"}`)},
		{Kind: websubhub.UpdateContent, Topic: topic, ContentType: "application/json", Body: []byte("not-json")},
	}
	for _, update := range tests {
		if _, err := hub.onUpdateMessage(context.Background(), update, websubhub.RequestMetadata{}); err == nil {
			t.Fatalf("update %+v unexpectedly accepted", update)
		}
	}
}

func TestEventNotificationFetchesJSONBeforePublishing(t *testing.T) {
	topicServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = io.WriteString(writer, `{"current":true}`)
	}))
	defer topicServer.Close()

	broker := newFakeBroker(websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topicServer.URL})
	hub := newTestHub(t, broker)
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:  websubhub.UpdateEvent,
		Topic: topicServer.URL,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish event notification: %v", err)
	}

	select {
	case published := <-broker.contentPublished:
		if published.topic != kafkaTopicName(topicServer.URL) ||
			published.content.ContentType != "application/json; charset=utf-8" ||
			string(published.content.Body) != `{"current":true}` {
			t.Errorf("materialized content = %+v", published)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for materialized Kafka content")
	}
}

func TestKafkaIdentifiersAreSafeAndCollisionResistant(t *testing.T) {
	first := kafkaTopicName("https://publisher.example/a/b")
	second := kafkaTopicName("https://publisher.example/a?b")
	if first == second {
		t.Fatalf("distinct topic URLs mapped to %q", first)
	}
	valid := regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	for _, identifier := range []string{
		first,
		second,
		subscriptionGroupName(verifiedSubscription(
			"https://publisher.example/a?token=secret",
			"https://subscriber.example/callback?capability=secret",
			time.Now(),
		)),
	} {
		if !valid.MatchString(identifier) || len(identifier) > 249 {
			t.Errorf("unsafe Kafka identifier %q", identifier)
		}
		if regexp.MustCompile(`token|capability|secret`).MatchString(identifier) {
			t.Errorf("Kafka identifier exposes URL data: %q", identifier)
		}
	}
}

func TestHubReplaysEventsFromEmptySnapshot(t *testing.T) {
	topic := "http://publisher.example/topics/replay"
	subscription := verifiedSubscription(
		topic,
		"http://subscriber.example/callback",
		time.Now(),
	)
	broker := newFakeBroker(websubhub.TopicRegistration{
		Mode:  websubhub.ModeRegister,
		Topic: topic,
	}, subscription)
	hub := newTestHubFromSnapshot(t, broker, state.Snapshot{})
	if !hub.hasTopic(topic) {
		t.Fatal("replayed topic is missing")
	}
	consumer := waitForConsumer(t, broker)
	if consumer.group != subscriptionGroupName(subscription) {
		t.Fatalf("consumer group = %q", consumer.group)
	}
}

func newTestHub(t *testing.T, broker *fakeBroker) *Hub {
	t.Helper()
	return newTestHubFromSnapshot(t, broker, snapshotFromEvents(broker.seed))
}

func newTestHubFromSnapshot(t *testing.T, broker *fakeBroker, snapshot state.Snapshot) *Hub {
	t.Helper()
	hub, err := New(context.Background(), Options{
		HubURL:           "http://hub.example/hub",
		ServerID:         testServerID,
		Broker:           broker,
		SnapshotSource:   staticSnapshotSource{snapshot: snapshot},
		DeliveryAttempts: 2,
		RetryBackoff:     time.Nanosecond,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
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

type staticSnapshotSource struct {
	snapshot state.Snapshot
}

func (s staticSnapshotSource) Snapshot(context.Context) (state.Snapshot, error) {
	return s.snapshot.Clone(), nil
}

func snapshotFromEvents(events []any) state.Snapshot {
	topics := make(map[string]websubhub.TopicRegistration)
	subscriptions := make(map[string]state.Subscription)
	for _, event := range events {
		switch event := event.(type) {
		case websubhub.TopicRegistration:
			topics[event.Topic] = event
		case websubhub.TopicDeregistration:
			delete(topics, event.Topic)
			for key, subscription := range subscriptions {
				if subscription.Topic == event.Topic {
					delete(subscriptions, key)
				}
			}
		case websubhub.VerifiedSubscription:
			subscriptions[event.Topic+"\x00"+event.Callback] = state.NewSubscription(event, testServerID)
		case state.Subscription:
			subscriptions[event.Topic+"\x00"+event.Callback] = event
		case websubhub.VerifiedUnsubscription:
			delete(subscriptions, event.Topic+"\x00"+event.Callback)
		}
	}
	snapshot := state.Snapshot{}
	for _, topic := range topics {
		snapshot.Topics = append(snapshot.Topics, topic)
	}
	for _, subscription := range subscriptions {
		snapshot.Subscriptions = append(snapshot.Subscriptions, subscription)
	}
	return snapshot
}
