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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

type receivedDelivery struct {
	body        string
	contentType string
	signature   string
	links       []string
}

func TestSubscriptionRenewalDeliveryAndUnsubscription(t *testing.T) {
	deliveries := make(chan receivedDelivery, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read delivery: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		deliveries <- receivedDelivery{
			body:        string(body),
			contentType: request.Header.Get("Content-Type"),
			signature:   request.Header.Get(websubhub.HeaderHubSignature),
			links:       request.Header.Values("Link"),
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer callback.Close()

	hub := newTestHub(t)
	topic := "http://publisher.example/topics/orders"
	registerTopic(t, hub, topic)
	verifySubscription(t, hub, topic, callback.URL, "old-secret")

	// A second verified request for the same topic/callback renews it and
	// replaces mutable subscription data such as the secret and lease.
	verifySubscription(t, hub, topic, callback.URL, "new-secret")
	if err := hub.onSubscriptionValidation(context.Background(), websubhub.Subscription{Topic: topic, Callback: callback.URL}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("renewal validation: %v", err)
	}

	payload := `{"order":"A-42"}`
	result, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: "application/json; charset=utf-8",
		Body:        []byte(payload),
	}, websubhub.RequestMetadata{})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if result.StatusCode != 0 {
		t.Fatalf("publish status = %d, want framework default", result.StatusCode)
	}

	select {
	case delivery := <-deliveries:
		if delivery.body != payload {
			t.Errorf("body = %q, want %q", delivery.body, payload)
		}
		if delivery.contentType != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", delivery.contentType)
		}
		mac := hmac.New(sha256.New, []byte("new-secret"))
		_, _ = mac.Write([]byte(payload))
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if delivery.signature != wantSignature {
			t.Errorf("signature = %q, want %q", delivery.signature, wantSignature)
		}
		joinedLinks := strings.Join(delivery.links, ",")
		if !strings.Contains(joinedLinks, `rel="hub"`) || !strings.Contains(joinedLinks, `rel="self"`) {
			t.Errorf("Link headers = %q", joinedLinks)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	unsubscription := websubhub.Unsubscription{Mode: websubhub.ModeUnsubscribe, Topic: topic, Callback: callback.URL}
	if err := hub.onUnsubscriptionValidation(context.Background(), unsubscription, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("unsubscription validation: %v", err)
	}
	if err := hub.onUnsubscriptionVerified(context.Background(), websubhub.VerifiedUnsubscription{Unsubscription: unsubscription}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("verified unsubscription: %v", err)
	}
	if hub.hasSubscription(topic, callback.URL) {
		t.Fatal("subscription remains after verified unsubscription")
	}
}

func TestEventNotificationFetchesTopicContent(t *testing.T) {
	topic := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
		_, _ = io.WriteString(writer, "<feed>updated</feed>")
	}))
	defer topic.Close()

	deliveries := make(chan receivedDelivery, 1)
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		deliveries <- receivedDelivery{body: string(body), contentType: request.Header.Get("Content-Type")}
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer callback.Close()

	hub := newTestHub(t)
	registerTopic(t, hub, topic.URL)
	verifySubscription(t, hub, topic.URL, callback.URL, "")
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:  websubhub.UpdateEvent,
		Topic: topic.URL,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.body != "<feed>updated</feed>" {
			t.Errorf("body = %q", delivery.body)
		}
		if delivery.contentType != "application/atom+xml; charset=utf-8" {
			t.Errorf("Content-Type = %q", delivery.contentType)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fetched topic delivery")
	}
}

func TestGoneCallbackRemovesOnlyCurrentSubscription(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusGone)
	}))
	defer callback.Close()

	hub := newTestHub(t)
	topic := "http://publisher.example/topics/deleted"
	registerTopic(t, hub, topic)
	verifySubscription(t, hub, topic, callback.URL, "")
	if _, err := hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{
		Kind:        websubhub.UpdateContent,
		Topic:       topic,
		ContentType: "text/plain",
		Body:        []byte("gone"),
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for hub.hasSubscription(topic, callback.URL) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hub.hasSubscription(topic, callback.URL) {
		t.Fatal("HTTP 410 callback was not removed")
	}
}

func TestRegistrationPolicy(t *testing.T) {
	hub := newTestHub(t)
	topic := "http://publisher.example/topics/orders"
	registerTopic(t, hub, topic)
	_, err := hub.onRegisterTopic(context.Background(), websubhub.TopicRegistration{Mode: websubhub.ModeRegister, Topic: topic}, websubhub.RequestMetadata{})
	if !errors.Is(err, websubhub.ErrDenied) {
		t.Fatalf("duplicate registration error = %v, want ErrDenied", err)
	}
	_, err = hub.onUpdateMessage(context.Background(), websubhub.UpdateMessage{Topic: "http://publisher.example/missing"}, websubhub.RequestMetadata{})
	if !errors.Is(err, websubhub.ErrDenied) {
		t.Fatalf("unknown topic update error = %v, want ErrDenied", err)
	}
}

func newTestHub(t *testing.T) *memoryHub {
	t.Helper()
	hub, err := newMemoryHub(hubOptions{
		hubURL:          "http://hub.example/hub",
		updateQueue:     8,
		deliveryQueue:   8,
		deliveryWorkers: 2,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new hub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := hub.Close(ctx); err != nil {
			t.Errorf("close hub: %v", err)
		}
	})
	return hub
}

func registerTopic(t *testing.T, hub *memoryHub, topic string) {
	t.Helper()
	if _, err := hub.onRegisterTopic(context.Background(), websubhub.TopicRegistration{
		Mode:  websubhub.ModeRegister,
		Topic: topic,
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("register topic: %v", err)
	}
}

func verifySubscription(t *testing.T, hub *memoryHub, topic, callback, secret string) {
	t.Helper()
	request := websubhub.Subscription{
		Hub:      "http://hub.example/hub",
		Mode:     websubhub.ModeSubscribe,
		Topic:    topic,
		Callback: callback,
		Secret:   secret,
	}
	if err := hub.onSubscriptionVerified(context.Background(), websubhub.VerifiedSubscription{
		Subscription:          request,
		EffectiveLeaseSeconds: "300",
		LeaseStartedAt:        time.Now(),
	}, websubhub.RequestMetadata{}); err != nil {
		t.Fatalf("verify subscription: %v", err)
	}
}
