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
	"sync"
	"testing"
	"time"
)

func minimumService() Service {
	return Service{
		OnSubscriptionVerified: func(context.Context, VerifiedSubscription, RequestMetadata) error { return nil },
		OnUnsubscriptionVerified: func(context.Context, VerifiedUnsubscription, RequestMetadata) error {
			return nil
		},
	}
}

func postForm(handler http.Handler, values url.Values) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "https://hub.example/hub", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestDecodeURLUnreserved(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"all unreserved classes", "https://example.com/%41%5a%61%7A%30%39%2d%2E%5f%7e", "https://example.com/AZaz09-._~"},
		{"reserved escapes", "https://example.com/%2f%3F%23?q=%26%3d%25", "https://example.com/%2f%3F%23?q=%26%3d%25"},
		{"non-ASCII bytes", "https://example.com/%C3%A9", "https://example.com/%C3%A9"},
		{"no escapes", "https://example.com/topic", "https://example.com/topic"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := decodeURLUnreserved(test.raw); got != test.want {
				t.Fatalf("decodeURLUnreserved(%q) = %q, want %q", test.raw, got, test.want)
			}
		})
	}
}

func TestAuthenticatedTLSSubscriptionLifecycle(t *testing.T) {
	type principalKey struct{}
	const principal = "alice"
	topic := "https://publisher.example/topics/7"
	callbackQuery := make(chan url.Values, 1)
	subscriber := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackQuery <- r.URL.Query()
		_, _ = io.WriteString(w, r.URL.Query().Get("hub.challenge"))
	}))
	defer subscriber.Close()

	verified := make(chan VerifiedSubscription, 1)
	metadataSeen := make(chan RequestMetadata, 1)
	service := minimumService()
	service.OnSubscription = func(ctx context.Context, request Subscription, metadata RequestMetadata, _ *Controller) (Result, error) {
		if ctx.Value(principalKey{}) != principal {
			t.Errorf("principal missing from callback context")
		}
		if request.Parameters.Get("tenant") != "blue" || request.Secret != "secret" {
			t.Errorf("application fields lost: %+v", request)
		}
		metadataSeen <- metadata
		return Result{}, nil
	}
	service.OnSubscriptionValidation = func(ctx context.Context, request Subscription, _ RequestMetadata) error {
		if ctx.Value(principalKey{}) != principal {
			t.Error("principal missing from asynchronous validation context")
		}
		return nil
	}
	service.OnSubscriptionVerified = func(ctx context.Context, subscription VerifiedSubscription, _ RequestMetadata) error {
		if ctx.Value(principalKey{}) != principal {
			t.Error("principal missing from verified callback context")
		}
		verified <- subscription
		return nil
	}
	handler, err := NewHandler(Config{
		HubURL: "https://hub.example/hub", HTTPClient: subscriber.Client(),
		MaxLease: 10 * time.Second, DefaultLease: 5 * time.Second,
	}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()

	auth := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer valid-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, principal)))
	})
	hub := httptest.NewTLSServer(auth)
	defer hub.Close()

	form := url.Values{
		"hub.mode": {"subscribe"}, "hub.topic": {topic},
		"hub.callback":      {subscriber.URL + "/callback?existing=yes"},
		"hub.lease_seconds": {"30"}, "hub.secret": {"secret"}, "tenant": {"blue"},
	}
	request, _ := http.NewRequest(http.MethodPost, hub.URL, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer valid-token")
	response, err := hub.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", response.StatusCode)
	}

	select {
	case query := <-callbackQuery:
		if query.Get("existing") != "yes" || query.Get("hub.mode") != "subscribe" ||
			query.Get("hub.topic") != topic || query.Get("hub.challenge") == "" ||
			query.Get("hub.lease_seconds") != "10" {
			t.Fatalf("verification query = %v", query)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verification callback was not called")
	}
	select {
	case got := <-verified:
		if got.EffectiveLeaseSeconds != "10" || got.Topic != topic || got.LeaseStartedAt.After(time.Now()) {
			t.Fatalf("verified subscription = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verified callback was not called")
	}
	if metadata := <-metadataSeen; metadata.Header.Get("Authorization") != "Bearer valid-token" {
		t.Fatalf("metadata did not preserve authentication header: %v", metadata.Header)
	}

	unauthorized, _ := http.NewRequest(http.MethodPost, hub.URL, strings.NewReader(form.Encode()))
	unauthorized.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	unauthorizedResponse, err := hub.Client().Do(unauthorized)
	if err != nil {
		t.Fatal(err)
	}
	defer unauthorizedResponse.Body.Close()
	if unauthorizedResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("outer auth middleware status = %d", unauthorizedResponse.StatusCode)
	}
}

func TestUnsubscriptionAndExternalVerification(t *testing.T) {
	callbackMode := make(chan string, 1)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackMode <- r.URL.Query().Get("hub.mode")
		_, _ = io.WriteString(w, r.URL.Query().Get("hub.challenge"))
	}))
	defer subscriber.Close()
	unverified := make(chan VerifiedUnsubscription, 1)
	service := minimumService()
	service.OnUnsubscriptionVerified = func(_ context.Context, request VerifiedUnsubscription, _ RequestMetadata) error {
		unverified <- request
		return nil
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example", HTTPClient: subscriber.Client()}, service)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"hub.mode": {"unsubscribe"}, "hub.topic": {"https://topic.example"}, "hub.callback": {subscriber.URL}}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("unsubscribe status = %d", response.Code)
	}
	select {
	case mode := <-callbackMode:
		if mode != "unsubscribe" {
			t.Fatalf("mode = %q", mode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unsubscribe verification missing")
	}
	select {
	case <-unverified:
	case <-time.After(2 * time.Second):
		t.Fatal("unsubscription verified callback missing")
	}
	if err = handler.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	verified := make(chan struct{}, 1)
	service = minimumService()
	service.OnSubscription = func(_ context.Context, _ Subscription, _ RequestMetadata, controller *Controller) (Result, error) {
		if err := controller.MarkVerified(); err != nil {
			t.Errorf("MarkVerified: %v", err)
		}
		if err := controller.MarkVerified(); !errors.Is(err, ErrAlreadyVerified) {
			t.Errorf("second MarkVerified = %v", err)
		}
		return Result{}, nil
	}
	service.OnSubscriptionVerified = func(context.Context, VerifiedSubscription, RequestMetadata) error {
		verified <- struct{}{}
		return nil
	}
	handler, err = NewHandler(Config{HubURL: "https://hub.example", AllowExternalVerification: true}, service)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"}, "hub.callback": {"http://127.0.0.1:1"}}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("external verification status = %d", response.Code)
	}
	select {
	case <-verified:
	case <-time.After(2 * time.Second):
		t.Fatal("externally verified callback missing")
	}
	_ = handler.Close(context.Background())
}

func TestValidationDenialNotification(t *testing.T) {
	notification := make(chan url.Values, 1)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notification <- r.URL.Query()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer subscriber.Close()
	verified := make(chan struct{}, 1)
	service := minimumService()
	service.OnSubscriptionValidation = func(context.Context, Subscription, RequestMetadata) error {
		return &DeniedError{Reason: "policy denied\r\nunsafe"}
	}
	service.OnSubscriptionVerified = func(context.Context, VerifiedSubscription, RequestMetadata) error {
		verified <- struct{}{}
		return nil
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example"}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()
	form := url.Values{"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"}, "hub.callback": {subscriber.URL}}
	if response := postForm(handler, form); response.Code != http.StatusAccepted {
		t.Fatalf("status = %d", response.Code)
	}
	select {
	case query := <-notification:
		if query.Get("hub.mode") != "denied" || query.Get("hub.reason") != "policy deniedunsafe" {
			t.Fatalf("denial = %v", query)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("denial notification missing")
	}
	select {
	case <-verified:
		t.Fatal("denied subscription was verified")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPublisherClientAgainstHandler(t *testing.T) {
	var mu sync.Mutex
	var registrations []string
	var updates []UpdateMessage
	service := minimumService()
	service.OnRegisterTopic = func(_ context.Context, request TopicRegistration, metadata RequestMetadata) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		registrations = append(registrations, "register:"+request.Topic+":"+request.ContentType)
		if metadata.Header.Get("Authorization") != "Bearer publisher" {
			t.Error("publisher metadata missing")
		}
		if request.Mode != ModeRegister {
			t.Errorf("registration mode = %q", request.Mode)
		}
		return Result{}, nil
	}
	service.OnDeregisterTopic = func(_ context.Context, request TopicDeregistration, _ RequestMetadata) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		if request.Mode != ModeDeregister {
			t.Errorf("deregistration mode = %q", request.Mode)
		}
		registrations = append(registrations, "deregister:"+request.Topic)
		return Result{}, nil
	}
	service.OnUpdateMessage = func(_ context.Context, message UpdateMessage, _ RequestMetadata) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		updates = append(updates, message)
		return Result{}, nil
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example", EnablePublisherExtension: true}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", "Bearer publisher")
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	publisher, err := NewPublisherClient(PublisherClientConfig{HubURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	topic := "https://publisher.example/%7Ealice?letter=%41&slash=%2F"
	normalizedTopic := "https://publisher.example/~alice?letter=A&slash=%2F"
	invalidTopic := postForm(handler, url.Values{"hub.mode": {"register"}, "hub.topic": {"test"}})
	if invalidTopic.Code != http.StatusBadRequest {
		t.Fatalf("relative topic registration status = %d, want %d", invalidTopic.Code, http.StatusBadRequest)
	}

	if err = publisher.RegisterTopic(context.Background(), topic, WithTopicContentType("application/ld+json; profile=\"https://example.com/profile\"")); err != nil {
		t.Fatal(err)
	}
	if err = publisher.DeregisterTopic(context.Background(), topic); err != nil {
		t.Fatal(err)
	}
	if err = publisher.Notify(context.Background(), topic); err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(context.Background(), UpdateMessage{Topic: topic, ContentType: "application/json", Body: []byte("{\"n\":1}")}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(registrations) != 2 || len(updates) != 2 {
		t.Fatalf("callbacks: registrations=%v updates=%+v", registrations, updates)
	}
	if registrations[0] != "register:"+normalizedTopic+":application/ld+json; profile=\"https://example.com/profile\"" ||
		registrations[1] != "deregister:"+normalizedTopic {
		t.Fatalf("normalized registration topics = %v", registrations)
	}

	if updates[0].Kind != UpdateEvent || updates[0].Topic != normalizedTopic || updates[1].Kind != UpdateContent ||
		updates[1].Topic != normalizedTopic || string(updates[1].Body) != "{\"n\":1}" {
		t.Fatalf("update mapping = %+v", updates)
	}
}

func TestRegistrationContentTypeValidation(t *testing.T) {
	received := make(chan TopicRegistration, 2)
	service := minimumService()
	service.OnRegisterTopic = func(_ context.Context, registration TopicRegistration, _ RequestMetadata) (Result, error) {
		received <- registration
		return Result{}, nil
	}
	service.OnDeregisterTopic = func(context.Context, TopicDeregistration, RequestMetadata) (Result, error) {
		return Result{}, nil
	}
	service.OnUpdateMessage = func(context.Context, UpdateMessage, RequestMetadata) (Result, error) {
		return Result{}, nil
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example", EnablePublisherExtension: true}, service)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close(context.Background()) }()

	topic := "https://publisher.example/topic"
	tests := []struct {
		name        string
		mode        string
		contentType []string
		status      int
		want        string
	}{
		{"omitted", "register", nil, http.StatusOK, ""},
		{"complete value", "register", []string{"application/ld+json; profile=\"https://example.com/profile\""}, http.StatusOK, "application/ld+json; profile=\"https://example.com/profile\""},
		{"empty", "register", []string{""}, http.StatusBadRequest, ""},
		{"duplicate", "register", []string{"text/plain", "application/json"}, http.StatusBadRequest, ""},
		{"malformed", "register", []string{"not a media type"}, http.StatusBadRequest, ""},
		{"deregistration", "deregister", []string{"text/plain"}, http.StatusBadRequest, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := url.Values{"hub.mode": {test.mode}, "hub.topic": {topic}}
			if test.contentType != nil {
				values["hub.content_type"] = test.contentType
			}
			response := postForm(handler, values)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if test.status == http.StatusOK {
				registration := <-received
				if registration.Mode != ModeRegister || registration.Topic != topic || registration.ContentType != test.want {
					t.Fatalf("registration = %+v", registration)
				}
			}
		})
	}
	if len(received) != 0 {
		t.Fatalf("invalid registration reached callback: %+v", <-received)
	}
}

func TestHandlerInputErrorsResultsAndClose(t *testing.T) {
	service := minimumService()
	service.OnSubscription = func(context.Context, Subscription, RequestMetadata, *Controller) (Result, error) {
		return Result{}, ErrDenied
	}
	handler, err := NewHandler(Config{HubURL: "https://hub.example", MaxRequestBody: 64}, service)
	if err != nil {
		t.Fatal(err)
	}
	valid := url.Values{"hub.mode": {"subscribe"}, "hub.topic": {"https://topic.example"}, "hub.callback": {"https://callback.example"}}
	if response := postForm(handler, valid); response.Code != http.StatusBadRequest {
		t.Fatalf("denied status = %d", response.Code)
	}
	cases := []struct {
		name   string
		method string
		body   string
		ctype  string
		status int
	}{
		{"method", http.MethodGet, "", "application/x-www-form-urlencoded", http.StatusMethodNotAllowed},
		{"missing content type", http.MethodPost, valid.Encode(), "", http.StatusBadRequest},
		{"bad media", http.MethodPost, valid.Encode(), "application/x-www-form-urlencoded; charset=iso-8859-1", http.StatusBadRequest},
		{"missing mode", http.MethodPost, "hub.topic=https%3A%2F%2Ftopic.example", "application/x-www-form-urlencoded", http.StatusBadRequest},
		{"duplicate", http.MethodPost, valid.Encode() + "&hub.topic=https%3A%2F%2Fother.example", "application/x-www-form-urlencoded", http.StatusBadRequest},
		{"bad lease", http.MethodPost, valid.Encode() + "&hub.lease_seconds=zero", "application/x-www-form-urlencoded", http.StatusBadRequest},
		{"too large", http.MethodPost, valid.Encode() + strings.Repeat("x", 100), "application/x-www-form-urlencoded", http.StatusBadRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "https://hub.example", strings.NewReader(test.body))
			if test.ctype != "" {
				request.Header.Set("Content-Type", test.ctype)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
	if err = handler.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if response := postForm(handler, valid); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed status = %d", response.Code)
	}
	if err = handler.Close(context.Background()); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}

	redirectService := minimumService()
	redirectService.OnSubscription = func(context.Context, Subscription, RequestMetadata, *Controller) (Result, error) {
		return Result{}, &RedirectError{StatusCode: http.StatusTemporaryRedirect, Location: "https://other.example/hub"}
	}
	redirectHandler, _ := NewHandler(Config{HubURL: "https://hub.example"}, redirectService)
	defer func() { _ = redirectHandler.Close(context.Background()) }()
	response := postForm(redirectHandler, valid)
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Location") != "https://other.example/hub" {
		t.Fatalf("redirect result: %d %v", response.Code, response.Header())
	}
}

func TestConstructorAndControllerValidation(t *testing.T) {
	service := minimumService()
	configs := []Config{
		{},
		{HubURL: "ftp://hub.example"},
		{HubURL: "https://hub.example", DefaultLease: 2 * time.Second, MaxLease: time.Second},
		{HubURL: "https://hub.example", MaxRequestBody: -1},
		{HubURL: "https://hub.example", DefaultLease: 500 * time.Millisecond},
	}
	for _, config := range configs {
		if _, err := NewHandler(config, service); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("config accepted: %+v, %v", config, err)
		}
	}
	if _, err := NewHandler(Config{HubURL: "https://hub.example"}, Service{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing callbacks accepted: %v", err)
	}
	if _, err := NewHandler(Config{HubURL: "https://hub.example", EnablePublisherExtension: true}, service); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing extension callbacks accepted: %v", err)
	}
	var controller *Controller
	if err := controller.MarkVerified(); !errors.Is(err, ErrExternalVerificationDisabled) {
		t.Fatalf("nil controller = %v", err)
	}
	controller = &Controller{}
	if err := controller.MarkVerified(); !errors.Is(err, ErrExternalVerificationDisabled) {
		t.Fatalf("disabled controller = %v", err)
	}
	denied := &DeniedError{Reason: "no"}
	if !errors.Is(denied, ErrDenied) || denied.Error() == "" {
		t.Fatalf("denied classification failed: %v", denied)
	}
	httpErr := &HTTPError{Operation: "test", StatusCode: 500, Err: ErrDeliveryFailed}
	if !errors.Is(httpErr, ErrDeliveryFailed) || httpErr.Error() == "" {
		t.Fatalf("HTTP error classification failed: %v", httpErr)
	}
}
