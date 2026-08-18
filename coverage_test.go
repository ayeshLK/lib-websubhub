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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPublisherHandlerFailureBranches(t *testing.T) {
	service := minimumService()
	service.OnRegisterTopic = func(context.Context, TopicRegistration, RequestMetadata) (Result, error) {
		return Result{}, &DeniedError{Reason: "not allowed"}
	}
	service.OnDeregisterTopic = func(context.Context, TopicDeregistration, RequestMetadata) (Result, error) {
		return Result{StatusCode: 199}, nil
	}
	service.OnUpdateMessage = func(context.Context, UpdateMessage, RequestMetadata) (Result, error) {
		panic("application bug")
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example", EnablePublisherExtension: true}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()

	register := url.Values{"hub.mode": {"register"}, "hub.topic": {"https://topic.example"}}
	response := postForm(handler, register)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "not+allowed") {
		t.Fatalf("denied registration: %d %q", response.Code, response.Body.String())
	}
	deregister := url.Values{"hub.mode": {"deregister"}, "hub.topic": {"https://topic.example"}}
	if response = postForm(handler, deregister); response.Code != http.StatusInternalServerError {
		t.Fatalf("invalid callback result status = %d", response.Code)
	}

	request := httptest.NewRequest(http.MethodPost,
		"https://hub.example?hub.mode=publish&hub.topic=https%3A%2F%2Ftopic.example",
		strings.NewReader("payload"))
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("X-Go-Publisher", "publish")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "operation+denied") {
		t.Fatalf("panicked update mapping: %d %q", response.Code, response.Body.String())
	}

	badRequests := []*http.Request{
		httptest.NewRequest(http.MethodPost, "https://hub.example", strings.NewReader("hub.mode=publish&hub.topic=https%3A%2F%2Ftopic.example")),
		httptest.NewRequest(http.MethodPost, "https://hub.example?hub.mode=publish", strings.NewReader("x")),
		httptest.NewRequest(http.MethodPost, "https://hub.example?hub.mode=no&hub.topic=https%3A%2F%2Ftopic.example", strings.NewReader("x")),
	}
	badRequests[0].Header.Set("Content-Type", "application/x-www-form-urlencoded")
	badRequests[1].Header.Set("Content-Type", "text/plain")
	badRequests[2].Header.Set("Content-Type", "text/plain")
	for _, bad := range badRequests {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, bad)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("bad publisher request status = %d", response.Code)
		}
	}
	event := httptest.NewRequest(http.MethodPost, "https://hub.example", strings.NewReader("hub.mode=register"))
	event.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	event.Header.Set(HeaderGoPublisher, "event")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, event)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid event status = %d", response.Code)
	}
}

func TestCallbackHTTPFailureBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			http.Error(w, "failed", http.StatusInternalServerError)
		case "/redirect":
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
		case "/large":
			_, _ = io.WriteString(w, "0123456789")
		default:
			_, _ = io.WriteString(w, "wrong")
		}
	}))
	defer server.Close()
	handler, err := NewHandler(Config{HubURL: "https://hub.example", MaxCallbackBody: 4}, minimumService())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()
	params := url.Values{"hub.mode": {"subscribe"}}
	for _, path := range []string{"/mismatch", "/status", "/redirect", "/large"} {
		err = handler.sendCallback(context.Background(), server.URL+path, params, "challenge")
		if !errors.Is(err, ErrVerificationFailed) {
			t.Fatalf("%s error = %v", path, err)
		}
	}
}

func TestUnsubscriptionCallbacksAndHubError(t *testing.T) {
	notifications := make(chan string, 2)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := r.URL.Query().Get("hub.mode")
		if mode == "unsubscribe" {
			_, _ = io.WriteString(w, r.URL.Query().Get("hub.challenge"))
			return
		}
		notifications <- mode
		w.WriteHeader(http.StatusNoContent)
	}))
	defer subscriber.Close()
	service := minimumService()
	service.OnUnsubscription = func(_ context.Context, request Unsubscription, _ RequestMetadata, controller *Controller) (Result, error) {
		if request.Parameters.Get("custom") != "value" {
			t.Errorf("custom parameter missing: %+v", request.Parameters)
		}
		if controller.isMarked() {
			t.Error("controller unexpectedly marked")
		}
		return Result{}, nil
	}
	service.OnUnsubscriptionValidation = func(context.Context, Unsubscription, RequestMetadata) error {
		return nil
	}
	service.OnUnsubscriptionVerified = func(context.Context, VerifiedUnsubscription, RequestMetadata) error {
		return errors.New("storage failed")
	}
	handler, err := NewHandler(Config{
		HubURL: "https://hub.example", EnableHubErrorCallback: true, HTTPClient: subscriber.Client(),
	}, service)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"hub.mode": {"unsubscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {subscriber.URL}, "custom": {"value"}}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("status = %d", response.Code)
	}
	select {
	case mode := <-notifications:
		if mode != "hub-error" {
			t.Fatalf("notification mode = %q", mode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hub-error notification missing")
	}
	_ = handler.Close(context.Background())
}

func TestVerificationQueueBackpressureAndCloseDeadline(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service := minimumService()
	service.OnSubscriptionValidation = func(ctx context.Context, _ Subscription, _ RequestMetadata) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return errors.New("stop")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	handler, err := NewHandler(Config{
		HubURL: "https://hub.example", VerificationWorkers: 1, VerificationQueue: 1,
		VerificationTimeout: time.Second,
	}, service)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"},
		"hub.callback": {"http://127.0.0.1:1"}}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("first status = %d", response.Code)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("queued status = %d", response.Code)
	}
	if response := postForm(handler, form); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("full queue status = %d", response.Code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err = handler.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close deadline error = %v", err)
	}
	close(release)
	if err = handler.Close(context.Background()); err != nil {
		t.Fatalf("final close = %v", err)
	}
}

func TestInternalValidationAndErrorTypes(t *testing.T) {
	if err := validateResult(Result{StatusCode: 600}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("bad status accepted: %v", err)
	}
	if err := validateResult(Result{Header: http.Header{"Connection": {"close"}}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("hop header accepted: %v", err)
	}
	if err := validateResult(Result{ContentType: "text/plain; charset=latin1"}); err != nil {
		t.Fatalf("valid arbitrary charset rejected: %v", err)
	}
	if err := validateResult(Result{ContentType: "not a media type"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("malformed content type accepted: %v", err)
	}
	if got := cloneHeader(nil); got != nil {
		t.Fatalf("nil header clone = %v", got)
	}
	if got := cloneValues(nil); got != nil {
		t.Fatalf("nil values clone = %v", got)
	}
	if _, err := validateHTTPURL("https://user@example.com", "test", true, false); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("userinfo accepted: %v", err)
	}
	if _, err := validateHTTPURL("https://example.com/#fragment", "test", false, true); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("fragment accepted: %v", err)
	}
	if _, err := readBounded(strings.NewReader("abc"), 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero bound accepted: %v", err)
	}
	if safeReason(strings.Repeat("x", maxSafeReasonBytes+20)) != strings.Repeat("x", maxSafeReasonBytes) {
		t.Fatal("safe reason was not bounded")
	}

	cause := errors.New("cause")
	denied := &DeniedError{Err: cause}
	if !errors.Is(denied, cause) || denied.Error() != ErrDenied.Error() {
		t.Fatalf("denied unwrap: %v", denied)
	}
	var nilDenied *DeniedError
	if nilDenied.Error() != ErrDenied.Error() || nilDenied.Unwrap() != nil {
		t.Fatal("nil denied methods")
	}
	var nilHTTP *HTTPError
	if nilHTTP.Error() == "" || nilHTTP.Unwrap() != nil {
		t.Fatal("nil HTTP error methods")
	}
	redirect := &RedirectError{StatusCode: http.StatusTemporaryRedirect}
	if redirect.Error() == "" {
		t.Fatal("redirect error string")
	}
	var nilRedirect *RedirectError
	if nilRedirect.Error() == "" {
		t.Fatal("nil redirect error string")
	}
}
