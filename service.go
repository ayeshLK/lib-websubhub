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
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

// Mode is a WebSub or project-specific hub operation.
type Mode string

const (
	ModeSubscribe   Mode = "subscribe"
	ModeUnsubscribe Mode = "unsubscribe"
	ModeRegister    Mode = "register"
	ModeDeregister  Mode = "deregister"
	ModePublish     Mode = "publish"
)

// UpdateKind distinguishes a content notification from an event-only notice.
type UpdateKind uint8

const (
	UpdateEvent UpdateKind = iota
	UpdateContent
)

// Config controls protocol behavior owned by Handler.
type Config struct {
	HubURL              string
	DefaultLease        time.Duration
	MaxLease            time.Duration
	MaxRequestBody      int64
	MaxCallbackBody     int64
	VerificationTimeout time.Duration
	VerificationWorkers int
	VerificationQueue   int
	HTTPClient          *http.Client
	Logger              *slog.Logger

	AllowExternalVerification bool
	EnablePublisherExtension  bool
	EnableHubErrorCallback    bool
}

// RequestMetadata is an immutable snapshot of relevant inbound HTTP metadata.
type RequestMetadata struct {
	Header     http.Header
	RemoteAddr string
}

// Result customizes an HTTP response produced for an initial callback.
type Result struct {
	StatusCode  int
	Header      http.Header
	ContentType string
	Body        []byte
}

// TopicRegistration describes the optional register operation.
type TopicRegistration struct {
	Mode  Mode
	Topic string
}

// TopicDeregistration describes the optional deregister operation.
type TopicDeregistration struct {
	Mode  Mode
	Topic string
}

// Subscription describes a subscriber's requested subscription.
type Subscription struct {
	Hub          string
	Mode         Mode
	Callback     string
	Topic        string
	LeaseSeconds string
	Secret       string
	Parameters   url.Values
}

// VerifiedSubscription describes successfully verified subscription intent.
type VerifiedSubscription struct {
	Subscription
	EffectiveLeaseSeconds string
	LeaseStartedAt        time.Time
}

// Unsubscription describes a subscriber's requested unsubscription.
type Unsubscription struct {
	Mode       Mode
	Callback   string
	Topic      string
	Secret     string
	Parameters url.Values
}

// VerifiedUnsubscription describes successfully verified unsubscription intent.
type VerifiedUnsubscription struct {
	Unsubscription
}

// UpdateMessage is publisher content or an event-only update notification.
type UpdateMessage struct {
	Kind        UpdateKind
	Topic       string
	ContentType string
	Body        []byte
	Header      http.Header
}

// RegisterTopicFunc handles the publisher registration extension.
type RegisterTopicFunc func(context.Context, TopicRegistration, RequestMetadata) (Result, error)

// DeregisterTopicFunc handles the publisher deregistration extension.
type DeregisterTopicFunc func(context.Context, TopicDeregistration, RequestMetadata) (Result, error)

// UpdateMessageFunc handles publisher content and event notifications.
type UpdateMessageFunc func(context.Context, UpdateMessage, RequestMetadata) (Result, error)

// SubscriptionFunc handles the synchronous subscription admission decision.
type SubscriptionFunc func(context.Context, Subscription, RequestMetadata, *Controller) (Result, error)

// SubscriptionValidationFunc validates an admitted subscription asynchronously.
type SubscriptionValidationFunc func(context.Context, Subscription, RequestMetadata) error

// SubscriptionVerifiedFunc receives successfully verified subscription intent.
type SubscriptionVerifiedFunc func(context.Context, VerifiedSubscription, RequestMetadata) error

// UnsubscriptionFunc handles the synchronous unsubscription admission decision.
type UnsubscriptionFunc func(context.Context, Unsubscription, RequestMetadata, *Controller) (Result, error)

// UnsubscriptionValidationFunc validates an admitted unsubscription asynchronously.
type UnsubscriptionValidationFunc func(context.Context, Unsubscription, RequestMetadata) error

// UnsubscriptionVerifiedFunc receives successfully verified unsubscription intent.
type UnsubscriptionVerifiedFunc func(context.Context, VerifiedUnsubscription, RequestMetadata) error

// Service contains application callbacks. All callbacks must be concurrency-safe.
type Service struct {
	OnRegisterTopic   RegisterTopicFunc
	OnDeregisterTopic DeregisterTopicFunc
	OnUpdateMessage   UpdateMessageFunc

	OnSubscription           SubscriptionFunc
	OnSubscriptionValidation SubscriptionValidationFunc
	OnSubscriptionVerified   SubscriptionVerifiedFunc

	OnUnsubscription           UnsubscriptionFunc
	OnUnsubscriptionValidation UnsubscriptionValidationFunc
	OnUnsubscriptionVerified   UnsubscriptionVerifiedFunc
}

// Controller controls explicitly enabled external intent verification.
type Controller struct {
	allowed bool
	marked  atomic.Bool
}

// MarkVerified records that the application performed equivalent intent
// verification. It is disabled unless Config.AllowExternalVerification is true.
func (c *Controller) MarkVerified() error {
	if c == nil || !c.allowed {
		return ErrExternalVerificationDisabled
	}
	if !c.marked.CompareAndSwap(false, true) {
		return ErrAlreadyVerified
	}
	return nil
}

func (c *Controller) isMarked() bool {
	return c != nil && c.marked.Load()
}
