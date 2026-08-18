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
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrInvalidRequest               = errors.New("websubhub: invalid request")
	ErrDenied                       = errors.New("websubhub: denied")
	ErrVerificationFailed           = errors.New("websubhub: verification failed")
	ErrSubscriptionGone             = errors.New("websubhub: subscription gone")
	ErrDeliveryFailed               = errors.New("websubhub: delivery failed")
	ErrPublisherFailed              = errors.New("websubhub: publisher request failed")
	ErrQueueFull                    = errors.New("websubhub: verification queue full")
	ErrClosed                       = errors.New("websubhub: closed")
	ErrExternalVerificationDisabled = errors.New("websubhub: external verification disabled")
	ErrAlreadyVerified              = errors.New("websubhub: already marked verified")
)

// HTTPError describes a bounded, safe non-success HTTP response.
type HTTPError struct {
	Operation  string
	StatusCode int
	Header     http.Header
	Body       []byte
	Err        error
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "websubhub: HTTP error"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("websubhub: %s returned HTTP %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("websubhub: %s failed", e.Operation)
}

// Unwrap returns the classified cause.
func (e *HTTPError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// DeniedError asks the handler to notify the subscriber that validation failed.
// Reason must be safe to disclose to the callback endpoint.
type DeniedError struct {
	Reason string
	Err    error
}

func (e *DeniedError) Error() string {
	if e == nil || e.Reason == "" {
		return ErrDenied.Error()
	}
	return ErrDenied.Error() + ": " + e.Reason
}

// Unwrap returns the application cause, if supplied.
func (e *DeniedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Is classifies every DeniedError as ErrDenied.
func (e *DeniedError) Is(target error) bool {
	return target == ErrDenied
}

// RedirectError requests a temporary redirect from an initial subscription callback.
type RedirectError struct {
	StatusCode int
	Location   string
}

func (e *RedirectError) Error() string {
	if e == nil {
		return "websubhub: redirect"
	}
	return fmt.Sprintf("websubhub: redirect with HTTP %d", e.StatusCode)
}

func invalidRequest(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, reason)
}
