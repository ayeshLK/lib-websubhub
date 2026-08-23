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
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestDeliveryClientWireAndResponse(t *testing.T) {
	payload := []byte("{\"hello\":\"world\"}")
	secret := "shared-secret"
	var received *http.Request
	var receivedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Clone(r.Context())
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Subscriber", "ok")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("stored"))
	}))
	defer server.Close()

	client, err := NewDeliveryClient(Subscription{
		Hub: "https://hub.example/hub", Topic: "https://publisher.example/topic",
		Callback: server.URL + "/callback?tenant=blue", Secret: secret,
	}, DeliveryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Deliver(context.Background(), ContentDistribution{
		ContentType: "application/json; charset=utf-8", Body: payload,
		Header: http.Header{"X-Trace": {"trace-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted || string(response.Body) != "stored" || response.Header.Get("X-Subscriber") != "ok" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if received.Method != http.MethodPost || received.URL.Query().Get("tenant") != "blue" {
		t.Fatalf("request target changed: %s %s", received.Method, received.URL)
	}
	if string(receivedBody) != string(payload) || received.Header.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("payload changed: %q %q", receivedBody, received.Header.Get("Content-Type"))
	}
	if received.Header.Get("X-Trace") != "trace-1" {
		t.Fatalf("custom headers missing: %v", received.Header)
	}
	if got := received.Header.Get("X-WebSubHub-Message-ID"); got != "" {
		t.Fatalf("non-standard message ID header = %q", got)
	}
	links := received.Header.Values("Link")
	if len(links) != 2 || !strings.Contains(strings.Join(links, ","), "rel=\"hub\"") || !strings.Contains(strings.Join(links, ","), "rel=\"self\"") {
		t.Fatalf("unexpected links: %v", links)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if received.Header.Get(HeaderHubSignature) != expected {
		t.Fatalf("signature = %q, want %q", received.Header.Get(HeaderHubSignature), expected)
	}
}

func TestDeliveryClientSignatureAlgorithms(t *testing.T) {
	payload := []byte("signed payload")
	secret := "shared-secret"
	tests := []struct {
		name      string
		algorithm SignatureAlgorithm
		prefix    string
		newHash   func() hash.Hash
	}{
		{name: "sha384", algorithm: SignatureSHA384, prefix: "sha384=", newHash: sha512.New384},
		{name: "sha512", algorithm: SignatureSHA512, prefix: "sha512=", newHash: sha512.New},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var signature string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				signature = r.Header.Get("X-Hub-Signature")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			client, err := NewDeliveryClient(Subscription{
				Hub: server.URL, Topic: server.URL + "/topic", Callback: server.URL, Secret: secret,
			}, DeliveryConfig{SignatureAlgorithm: test.algorithm})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.Deliver(context.Background(), ContentDistribution{
				ContentType: "text/plain", Body: payload,
			}); err != nil {
				t.Fatal(err)
			}
			mac := hmac.New(test.newHash, []byte(secret))
			_, _ = mac.Write(payload)
			want := test.prefix + hex.EncodeToString(mac.Sum(nil))
			if signature != want {
				t.Fatalf("signature = %q, want %q", signature, want)
			}
		})
	}
}

func TestDeliveryClientErrorsAndTLS(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		gone   bool
		limit  int64
	}{
		{name: "gone", status: http.StatusGone, body: "gone", gone: true},
		{name: "failure", status: http.StatusBadGateway, body: "bad"},
		{name: "bounded", status: http.StatusOK, body: "too large", limit: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			client, err := NewDeliveryClient(Subscription{
				Hub: server.URL, Topic: server.URL + "/topic", Callback: server.URL,
			}, DeliveryConfig{MaxResponseBody: test.limit})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Deliver(context.Background(), ContentDistribution{ContentType: "text/plain"})
			if !errors.Is(err, ErrDeliveryFailed) {
				t.Fatalf("error = %v", err)
			}
			if errors.Is(err, ErrSubscriptionGone) != test.gone {
				t.Fatalf("gone classification = %v", err)
			}
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) {
				t.Fatalf("expected HTTPError, got %T", err)
			}
		})
	}

	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer tlsServer.Close()
	sub := Subscription{Hub: tlsServer.URL, Topic: tlsServer.URL + "/topic", Callback: tlsServer.URL}
	trusted, err := NewDeliveryClient(sub, DeliveryConfig{HTTPClient: tlsServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = trusted.Deliver(context.Background(), ContentDistribution{ContentType: "text/plain"}); err != nil {
		t.Fatalf("injected TLS client failed: %v", err)
	}
	untrusted, err := NewDeliveryClient(sub, DeliveryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = untrusted.Deliver(context.Background(), ContentDistribution{ContentType: "text/plain"}); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("default client unexpectedly trusted test CA: %v", err)
	}
}

func TestDeliveryValidation(t *testing.T) {
	valid := Subscription{Hub: "https://hub.example", Topic: "https://topic.example", Callback: "https://callback.example"}
	cases := []Subscription{
		{Hub: "ftp://hub.example", Topic: valid.Topic, Callback: valid.Callback},
		{Hub: valid.Hub, Topic: "relative", Callback: valid.Callback},
		{Hub: valid.Hub, Topic: valid.Topic, Callback: "https://user@callback.example"},
		{Hub: valid.Hub, Topic: valid.Topic, Callback: valid.Callback, Secret: strings.Repeat("s", 200)},
	}
	for _, sub := range cases {
		if _, err := NewDeliveryClient(sub, DeliveryConfig{}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("expected invalid request for %+v: %v", sub, err)
		}
	}
	if _, err := NewDeliveryClient(valid, DeliveryConfig{Timeout: -1}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("negative timeout accepted: %v", err)
	}
	if _, err := NewDeliveryClient(valid, DeliveryConfig{SignatureAlgorithm: "md5"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unsupported signature algorithm accepted: %v", err)
	}
	var nilClient *DeliveryClient
	if _, err := nilClient.Deliver(context.Background(), ContentDistribution{ContentType: "text/plain"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil client error = %v", err)
	}
	client, _ := NewDeliveryClient(valid, DeliveryConfig{})
	for _, content := range []ContentDistribution{
		{},
		{ContentType: "text/plain", Header: http.Header{"Link": {"bad"}}},
		{ContentType: "text/plain", Header: http.Header{"X-Test": {"bad\rvalue"}}},
	} {
		if _, err := client.Deliver(context.Background(), content); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid content accepted: %+v, %v", content, err)
		}
	}
}

func TestPublisherClientWire(t *testing.T) {
	if HeaderGoPublisher != "X-Go-Publisher" {
		t.Fatalf("publisher header = %q", HeaderGoPublisher)
	}
	type seenRequest struct {
		mode, topic, extension, contentType, body string
		query                                     url.Values
		header                                    http.Header
	}
	seen := make(chan seenRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		values, _ := url.ParseQuery(string(body))
		mode, topic := values.Get("hub.mode"), values.Get("hub.topic")
		if r.URL.Query().Get("hub.mode") != "" {
			mode, topic = r.URL.Query().Get("hub.mode"), r.URL.Query().Get("hub.topic")
		}
		seen <- seenRequest{mode: mode, topic: topic, extension: r.Header.Get("X-Go-Publisher"),
			contentType: r.Header.Get("Content-Type"), body: string(body), query: r.URL.Query(), header: r.Header.Clone()}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("hub.mode=accepted"))
	}))
	defer server.Close()
	client, err := NewPublisherClient(PublisherClientConfig{HubURL: server.URL + "?tenant=one"})
	if err != nil {
		t.Fatal(err)
	}
	topic := "https://publisher.example/topic"
	if err = client.RegisterTopic(context.Background(), topic); err != nil {
		t.Fatal(err)
	}
	if err = client.DeregisterTopic(context.Background(), topic); err != nil {
		t.Fatal(err)
	}
	if err = client.Notify(context.Background(), topic); err != nil {
		t.Fatal(err)
	}
	if err = client.Publish(context.Background(), UpdateMessage{
		Topic: topic, ContentType: "application/json; charset=utf-8", Body: []byte("{}"), Header: http.Header{"X-Trace": {"7"}},
	}); err != nil {
		t.Fatal(err)
	}
	got := []seenRequest{<-seen, <-seen, <-seen, <-seen}
	if got[0].mode != "register" || got[1].mode != "deregister" {
		t.Fatalf("registration modes: %+v", got)
	}
	if got[2].mode != "publish" || got[2].extension != "event" {
		t.Fatalf("notify wire: %+v", got[2])
	}
	if got[3].mode != "publish" || got[3].extension != "publish" || got[3].body != "{}" ||
		got[3].contentType != "application/json; charset=utf-8" || got[3].header.Get("X-Trace") != "7" ||
		got[3].query.Get("tenant") != "one" {
		t.Fatalf("publish wire: %+v", got[3])
	}
}

func TestPublisherErrorsAndValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("case") {
		case "status":
			http.Error(w, "no", http.StatusBadRequest)
		case "large":
			_, _ = w.Write([]byte("hub.mode=accepted-and-too-large"))
		default:
			_, _ = w.Write([]byte("hub.mode=denied"))
		}
	}))
	defer server.Close()
	for _, suffix := range []string{"", "?case=status"} {
		client, _ := NewPublisherClient(PublisherClientConfig{HubURL: server.URL + suffix})
		err := client.RegisterTopic(context.Background(), "https://topic.example")
		if !errors.Is(err, ErrPublisherFailed) {
			t.Fatalf("publisher error = %v", err)
		}
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) {
			t.Fatalf("expected HTTPError: %v", err)
		}
	}
	client, _ := NewPublisherClient(PublisherClientConfig{HubURL: server.URL + "?case=large", MaxResponseBody: 4})
	if err := client.Notify(context.Background(), "https://topic.example"); !errors.Is(err, ErrPublisherFailed) {
		t.Fatalf("bounded error = %v", err)
	}
	if _, err := NewPublisherClient(PublisherClientConfig{HubURL: "relative"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid hub accepted: %v", err)
	}
	if _, err := NewPublisherClient(PublisherClientConfig{HubURL: server.URL, MaxResponseBody: -1}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("negative limit accepted: %v", err)
	}
	var nilClient *PublisherClient
	if err := nilClient.Notify(context.Background(), "https://topic.example"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil publisher error = %v", err)
	}
	valid, _ := NewPublisherClient(PublisherClientConfig{HubURL: server.URL})
	if err := valid.Publish(context.Background(), UpdateMessage{Topic: "bad", ContentType: "text/plain"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid topic accepted: %v", err)
	}
	if err := valid.RegisterTopic(context.Background(), "test"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("relative registration topic accepted: %v", err)
	}
	if err := valid.Publish(context.Background(), UpdateMessage{Topic: "https://topic.example", ContentType: "text/plain", Header: http.Header{HeaderGoPublisher: {"event"}}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("reserved header accepted: %v", err)
	}
}

func TestAddDiscoveryLinks(t *testing.T) {
	header := http.Header{"Link": {"<https://alternate.example>; rel=\"alternate\""}}
	if err := AddDiscoveryLinks(header, "https://publisher.example/topic", "https://hub-one.example", "https://hub-two.example"); err != nil {
		t.Fatal(err)
	}
	if len(header.Values("Link")) != 4 {
		t.Fatalf("links were overwritten: %v", header.Values("Link"))
	}
	before := header.Clone()
	if err := AddDiscoveryLinks(header, "https://publisher.example/topic", "not-a-url"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("bad hub accepted: %v", err)
	}
	if len(header.Values("Link")) != len(before.Values("Link")) {
		t.Fatal("validation partially mutated headers")
	}
	if err := AddDiscoveryLinks(nil, "https://topic.example", "https://hub.example"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil header accepted: %v", err)
	}
	if err := AddDiscoveryLinks(http.Header{}, "https://topic.example"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty hubs accepted: %v", err)
	}
}
