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
	// ModeSubscribe requests or represents a WebSub subscription.
	ModeSubscribe Mode = "subscribe"
	// ModeUnsubscribe requests or represents a WebSub unsubscription.
	ModeUnsubscribe Mode = "unsubscribe"
	// ModeRegister is the optional publisher-extension topic registration mode.
	ModeRegister Mode = "register"
	// ModeDeregister is the optional publisher-extension topic deregistration mode.
	ModeDeregister Mode = "deregister"
	// ModePublish is the optional publisher-extension update mode.
	ModePublish Mode = "publish"
)

// UpdateKind distinguishes a content notification from an event-only notice.
type UpdateKind uint8

const (
	// UpdateEvent asks the application to fetch the current topic representation.
	UpdateEvent UpdateKind = iota
	// UpdateContent carries an exact publisher-supplied representation.
	UpdateContent
)

// Config controls protocol behavior owned by Handler. Zero values select
// bounded package defaults unless a field says otherwise.
type Config struct {
	// HubURL is the public absolute HTTP(S) URL subscribers use for this hub.
	HubURL string
	// DefaultLease applies when a subscription omits hub.lease_seconds.
	DefaultLease time.Duration
	// MaxLease caps requested and default subscription leases.
	MaxLease time.Duration
	// MaxRequestBody bounds inbound hub request bodies in bytes.
	MaxRequestBody int64
	// MaxCallbackBody bounds intent-verification and status callback responses.
	MaxCallbackBody int64
	// VerificationTimeout bounds each outbound verification operation.
	VerificationTimeout time.Duration
	// VerificationWorkers is the fixed asynchronous verification worker count.
	VerificationWorkers int
	// VerificationQueue is the maximum queued verification job count.
	VerificationQueue int
	// HTTPClient supplies outbound transport settings. It is copied, never mutated,
	// and configured to refuse redirects.
	HTTPClient *http.Client
	// Logger receives secret-safe lifecycle messages. Nil disables logging.
	Logger *slog.Logger

	// AllowExternalVerification enables Controller.MarkVerified during admission.
	AllowExternalVerification bool
	// EnablePublisherExtension enables non-standard publisher operations.
	EnablePublisherExtension bool
	// EnableHubErrorCallback sends hub-error status after unexpected asynchronous
	// validation or verified-callback failures.
	EnableHubErrorCallback bool
}

// RequestMetadata is a detached snapshot of relevant inbound HTTP metadata.
type RequestMetadata struct {
	// Header is a detached copy of the inbound request headers.
	Header http.Header
	// RemoteAddr is the inbound request's network address.
	RemoteAddr string
}

// Result customizes the synchronous HTTP response produced by an admission or
// publisher-extension callback. Its mutable fields are copied before use.
type Result struct {
	// StatusCode selects the response status; zero uses the operation default.
	StatusCode int
	// Header supplies additional response headers, subject to safety validation.
	Header http.Header
	// ContentType supplies the response media type when Body is present.
	ContentType string
	// Body supplies exact response bytes.
	Body []byte
}

// TopicRegistration describes an optional publisher-extension registration.
type TopicRegistration struct {
	// Mode is ModeRegister.
	Mode Mode
	// Topic is the absolute HTTP(S) topic URL after percent-encoded unreserved
	// characters are decoded.
	Topic string
}

// TopicDeregistration describes an optional publisher-extension deregistration.
type TopicDeregistration struct {
	// Mode is ModeDeregister.
	Mode Mode
	// Topic is the absolute HTTP(S) topic URL after percent-encoded unreserved
	// characters are decoded.
	Topic string
}

// Subscription describes a subscriber's requested subscription. Mutable values
// are detached from the inbound request before callbacks receive them.
type Subscription struct {
	// Hub is the configured public hub URL.
	Hub string
	// Mode is ModeSubscribe.
	Mode Mode
	// Callback is the absolute HTTP(S) subscriber callback URL after
	// percent-encoded unreserved characters are decoded.
	Callback string
	// Topic is the absolute HTTP(S) topic URL after percent-encoded unreserved
	// characters are decoded.
	Topic string
	// LeaseSeconds is the optional hub.lease_seconds form value.
	LeaseSeconds string
	// Secret is the optional hub.secret form value and must remain confidential.
	Secret string
	// Parameters contains detached, non-standard form parameters.
	Parameters url.Values
}

// VerifiedSubscription describes successfully verified subscription intent.
type VerifiedSubscription struct {
	Subscription
	// EffectiveLeaseSeconds is the granted lease after defaults and caps.
	EffectiveLeaseSeconds string
	// LeaseStartedAt records when verification work selected the effective lease.
	LeaseStartedAt time.Time
}

// Unsubscription describes a subscriber's requested unsubscription.
type Unsubscription struct {
	// Mode is ModeUnsubscribe.
	Mode Mode
	// Callback is the absolute HTTP(S) subscriber callback URL after
	// percent-encoded unreserved characters are decoded.
	Callback string
	// Topic is the absolute HTTP(S) topic URL after percent-encoded unreserved
	// characters are decoded.
	Topic string
	// Secret is the optional hub.secret form value and must remain confidential.
	Secret string
	// Parameters contains detached, non-standard form parameters.
	Parameters url.Values
}

// VerifiedUnsubscription describes successfully verified unsubscription
// intent. Any submitted lease value was accepted and ignored before this value
// was constructed.
type VerifiedUnsubscription struct {
	Unsubscription
}

// UpdateMessage is publisher content or an event-only update notification.
type UpdateMessage struct {
	// Kind distinguishes exact content from an event-only notification.
	Kind UpdateKind
	// Topic is the absolute HTTP(S) topic URL after percent-encoded unreserved
	// characters are decoded for inbound publisher messages.
	Topic string
	// ContentType is the complete publisher media type for UpdateContent.
	ContentType string
	// Body contains exact publisher bytes for UpdateContent.
	Body []byte
	// Header is a detached copy of publisher request headers.
	Header http.Header
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

// Service contains application callbacks. All callbacks may run concurrently
// and must be concurrency-safe.
type Service struct {
	// OnRegisterTopic handles optional publisher-extension topic registration.
	OnRegisterTopic RegisterTopicFunc
	// OnDeregisterTopic handles optional publisher-extension topic deregistration.
	OnDeregisterTopic DeregisterTopicFunc
	// OnUpdateMessage handles optional publisher content and event notifications.
	OnUpdateMessage UpdateMessageFunc

	// OnSubscription makes the synchronous subscription admission decision. Nil
	// accepts a valid request using the default response.
	OnSubscription SubscriptionFunc
	// OnSubscriptionValidation performs asynchronous application validation.
	OnSubscriptionValidation SubscriptionValidationFunc
	// OnSubscriptionVerified persists successfully verified subscription state.
	// It is required.
	OnSubscriptionVerified SubscriptionVerifiedFunc

	// OnUnsubscription makes the synchronous unsubscription admission decision.
	// Nil accepts a valid request using the default response.
	OnUnsubscription UnsubscriptionFunc
	// OnUnsubscriptionValidation performs asynchronous application validation.
	OnUnsubscriptionValidation UnsubscriptionValidationFunc
	// OnUnsubscriptionVerified removes successfully verified subscription state.
	// It is required.
	OnUnsubscriptionVerified UnsubscriptionVerifiedFunc
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
