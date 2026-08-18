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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestAdmissionStatusIsExactlyAccepted(t *testing.T) {
	tests := []struct {
		name       string
		mode       Mode
		statusCode int
		want       int
	}{
		{name: "subscription default", mode: ModeSubscribe, want: http.StatusAccepted},
		{name: "subscription explicit accepted", mode: ModeSubscribe, statusCode: http.StatusAccepted, want: http.StatusAccepted},
		{name: "subscription ok invalid", mode: ModeSubscribe, statusCode: http.StatusOK, want: http.StatusInternalServerError},
		{name: "subscription no content invalid", mode: ModeSubscribe, statusCode: http.StatusNoContent, want: http.StatusInternalServerError},
		{name: "subscription redirect result invalid", mode: ModeSubscribe, statusCode: http.StatusTemporaryRedirect, want: http.StatusInternalServerError},
		{name: "subscription rejection", mode: ModeSubscribe, statusCode: http.StatusForbidden, want: http.StatusForbidden},
		{name: "unsubscription default", mode: ModeUnsubscribe, want: http.StatusAccepted},
		{name: "unsubscription explicit accepted", mode: ModeUnsubscribe, statusCode: http.StatusAccepted, want: http.StatusAccepted},
		{name: "unsubscription ok invalid", mode: ModeUnsubscribe, statusCode: http.StatusOK, want: http.StatusInternalServerError},
		{name: "unsubscription rejection", mode: ModeUnsubscribe, statusCode: http.StatusUnauthorized, want: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := minimumService()
			service.OnSubscription = func(_ context.Context, _ Subscription, _ RequestMetadata, controller *Controller) (Result, error) {
				_ = controller.MarkVerified()
				return Result{StatusCode: test.statusCode}, nil
			}
			service.OnUnsubscription = func(_ context.Context, _ Unsubscription, _ RequestMetadata, controller *Controller) (Result, error) {
				_ = controller.MarkVerified()
				return Result{StatusCode: test.statusCode}, nil
			}
			handler, err := NewHandler(Config{
				HubURL: "https://hub.example", AllowExternalVerification: true,
			}, service)
			if err != nil {
				t.Fatal(err)
			}
			form := url.Values{
				"hub.mode": {string(test.mode)}, "hub.topic": {"https://topic.example"},
				"hub.callback": {"https://callback.example"},
			}
			response := postForm(handler, form)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
			if err = handler.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnsubscriptionRedirects(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			service := minimumService()
			service.OnUnsubscription = func(context.Context, Unsubscription, RequestMetadata, *Controller) (Result, error) {
				return Result{}, &RedirectError{StatusCode: status, Location: "https://other.example/hub"}
			}
			handler, err := NewHandler(Config{HubURL: "https://hub.example"}, service)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = handler.Close(context.Background()) }()
			response := postForm(handler, url.Values{
				"hub.mode": {"unsubscribe"}, "hub.topic": {"https://topic.example"},
				"hub.callback": {"https://callback.example"},
			})
			if response.Code != status || response.Header().Get("Location") != "https://other.example/hub" {
				t.Fatalf("redirect = %d %q", response.Code, response.Header().Get("Location"))
			}
		})
	}
}

func TestVerificationPreservesRawCallbackQuery(t *testing.T) {
	const originalQuery = "z=a%20b&x=1&x=2&empty=&hub.mode=original"
	requestURI := make(chan string, 1)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURI <- r.RequestURI
		challenges := r.URL.Query()["hub.challenge"]
		if len(challenges) != 1 {
			http.Error(w, "challenge missing", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, challenges[0])
	}))
	defer subscriber.Close()

	verified := make(chan struct{}, 1)
	service := minimumService()
	service.OnSubscriptionVerified = func(context.Context, VerifiedSubscription, RequestMetadata) error {
		verified <- struct{}{}
		return nil
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example"}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()
	response := postForm(handler, url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {subscriber.URL + "/callback?" + originalQuery},
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d", response.Code)
	}
	select {
	case raw := <-requestURI:
		wantPrefix := "/callback?" + originalQuery + "&"
		if !strings.HasPrefix(raw, wantPrefix) {
			t.Fatalf("raw callback query changed: %q", raw)
		}
		if !strings.Contains(raw, "hub.challenge=") || !strings.Contains(raw, "hub.lease_seconds=") {
			t.Fatalf("verification parameters missing: %q", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verification request missing")
	}
	select {
	case <-verified:
	case <-time.After(2 * time.Second):
		t.Fatal("verified callback missing")
	}
}

func TestProtocolStringFieldsAndIgnoredUnsubscriptionLease(t *testing.T) {
	subscriptionSeen := make(chan Subscription, 1)
	unsubscriptionSeen := make(chan Unsubscription, 1)
	verified := make(chan VerifiedSubscription, 1)
	service := minimumService()
	service.OnSubscription = func(_ context.Context, message Subscription, _ RequestMetadata, controller *Controller) (Result, error) {
		subscriptionSeen <- message
		_ = controller.MarkVerified()
		return Result{}, nil
	}
	service.OnSubscriptionVerified = func(_ context.Context, message VerifiedSubscription, _ RequestMetadata) error {
		verified <- message
		return nil
	}
	service.OnUnsubscription = func(_ context.Context, message Unsubscription, _ RequestMetadata, controller *Controller) (Result, error) {
		unsubscriptionSeen <- message
		_ = controller.MarkVerified()
		return Result{}, nil
	}
	handler, err := NewHandler(Config{
		HubURL: "https://hub.example", DefaultLease: 5 * time.Second,
		MaxLease: 10 * time.Second, AllowExternalVerification: true,
	}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()

	subscribe := postForm(handler, url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {"https://callback.example"}, "hub.lease_seconds": {"30"},
		"hub.secret": {"shared-secret"},
	})
	if subscribe.Code != http.StatusAccepted {
		t.Fatalf("subscribe status = %d", subscribe.Code)
	}
	message := <-subscriptionSeen
	if message.Hub != "https://hub.example" || message.Mode != ModeSubscribe ||
		message.LeaseSeconds != "30" || message.Secret != "shared-secret" {
		t.Fatalf("subscription fields = %+v", message)
	}
	verifiedMessage := <-verified
	if verifiedMessage.EffectiveLeaseSeconds != "10" || verifiedMessage.LeaseStartedAt.IsZero() {
		t.Fatalf("verified fields = %+v", verifiedMessage)
	}

	unsubscribe := postForm(handler, url.Values{
		"hub.mode": {"unsubscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {"https://callback.example"}, "hub.lease_seconds": {"not-a-number"},
	})
	if unsubscribe.Code != http.StatusAccepted {
		t.Fatalf("unsubscribe status = %d", unsubscribe.Code)
	}
	unsubscription := <-unsubscriptionSeen
	if unsubscription.Mode != ModeUnsubscribe || unsubscription.Parameters.Has("hub.lease_seconds") {
		t.Fatalf("unsubscription fields = %+v", unsubscription)
	}

	emptySecret := postForm(handler, url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {"https://callback.example"}, "hub.secret": {""},
	})
	if emptySecret.Code != http.StatusBadRequest {
		t.Fatalf("empty secret status = %d", emptySecret.Code)
	}
}
