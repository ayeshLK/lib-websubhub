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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (h *Handler) runSubscription(ctx context.Context, job verificationJob) {
	message := cloneSubscription(job.sub)
	if callback := h.service.OnSubscriptionValidation; callback != nil {
		if err := callSubscriptionValidation(callback, ctx, cloneSubscription(message), cloneMetadata(job.metadata)); err != nil {
			h.handleValidationFailure(ctx, message.Callback, message.Topic, err)
			return
		}
	}

	lease := h.config.DefaultLease
	if message.LeaseSeconds != "" {
		seconds, _ := strconv.ParseInt(message.LeaseSeconds, 10, 64)
		lease = time.Duration(seconds) * time.Second
	}
	if lease > h.config.MaxLease {
		lease = h.config.MaxLease
	}
	effectiveLeaseSeconds := strconv.FormatInt(int64(lease/time.Second), 10)
	leaseStartedAt := time.Now()
	if !job.verified {
		challenge, err := newChallenge()
		if err != nil {
			h.logger.Error("challenge generation failed", "operation", "subscribe")
			return
		}
		parameters := url.Values{
			"hub.mode":          {string(ModeSubscribe)},
			"hub.topic":         {message.Topic},
			"hub.challenge":     {challenge},
			"hub.lease_seconds": {effectiveLeaseSeconds},
		}
		if err = h.verifyIntent(ctx, message.Callback, parameters, challenge); err != nil {
			h.logger.Info("subscription intent verification failed", "operation", "subscribe")
			return
		}
	}

	verified := VerifiedSubscription{
		Subscription:          cloneSubscription(message),
		EffectiveLeaseSeconds: effectiveLeaseSeconds,
		LeaseStartedAt:        leaseStartedAt,
	}
	if err := callSubscriptionVerified(h.service.OnSubscriptionVerified, ctx, verified, cloneMetadata(job.metadata)); err != nil {
		h.logger.Error("verified callback failed", "operation", "subscribe")
		if h.config.EnableHubErrorCallback {
			h.sendStatusNotification(ctx, message.Callback, "hub-error", message.Topic, "verified callback failed")
		}
	}
}

func (h *Handler) runUnsubscription(ctx context.Context, job verificationJob) {
	message := cloneUnsubscription(job.unsub)
	if callback := h.service.OnUnsubscriptionValidation; callback != nil {
		if err := callUnsubscriptionValidation(callback, ctx, cloneUnsubscription(message), cloneMetadata(job.metadata)); err != nil {
			h.handleValidationFailure(ctx, message.Callback, message.Topic, err)
			return
		}
	}

	if !job.verified {
		challenge, err := newChallenge()
		if err != nil {
			h.logger.Error("challenge generation failed", "operation", "unsubscribe")
			return
		}
		parameters := url.Values{
			"hub.mode":      {string(ModeUnsubscribe)},
			"hub.topic":     {message.Topic},
			"hub.challenge": {challenge},
		}
		if err = h.verifyIntent(ctx, message.Callback, parameters, challenge); err != nil {
			h.logger.Info("subscription intent verification failed", "operation", "unsubscribe")
			return
		}
	}

	verified := VerifiedUnsubscription{Unsubscription: cloneUnsubscription(message)}
	if err := callUnsubscriptionVerified(h.service.OnUnsubscriptionVerified, ctx, verified, cloneMetadata(job.metadata)); err != nil {
		h.logger.Error("verified callback failed", "operation", "unsubscribe")
		if h.config.EnableHubErrorCallback {
			h.sendStatusNotification(ctx, message.Callback, "hub-error", message.Topic, "verified callback failed")
		}
	}
}

func (h *Handler) handleValidationFailure(ctx context.Context, callback, topic string, err error) {
	var denied *DeniedError
	if errors.As(err, &denied) {
		h.sendStatusNotification(ctx, callback, "denied", topic, safeReason(denied.Reason))
		return
	}
	h.logger.Error("validation callback failed")
	if h.config.EnableHubErrorCallback {
		h.sendStatusNotification(ctx, callback, "hub-error", topic, "validation failed")
	}
}

func (h *Handler) sendStatusNotification(ctx context.Context, callback, mode, topic, reason string) {
	parameters := url.Values{
		"hub.mode":  {mode},
		"hub.topic": {topic},
	}
	if reason != "" {
		parameters.Set("hub.reason", safeReason(reason))
	}
	_ = h.sendCallback(ctx, callback, parameters, "")
}

func (h *Handler) verifyIntent(ctx context.Context, callback string, parameters url.Values, challenge string) error {
	return h.sendCallback(ctx, callback, parameters, challenge)
}

func (h *Handler) sendCallback(ctx context.Context, callback string, parameters url.Values, expectedBody string) error {
	target, err := url.Parse(callback)
	if err != nil {
		return fmt.Errorf("%w: callback URL", ErrVerificationFailed)
	}
	encodedParameters := parameters.Encode()
	if target.RawQuery == "" {
		target.RawQuery = encodedParameters
	} else {
		target.RawQuery += "&" + encodedParameters
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return fmt.Errorf("%w: request creation", ErrVerificationFailed)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: callback request", ErrVerificationFailed)
	}
	body, readErr := responseSnapshot(response, h.config.MaxCallbackBody, "intent verification", ErrVerificationFailed)
	if readErr != nil {
		return readErr
	}
	if expectedBody == "" {
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &HTTPError{
			Operation:  "intent verification",
			StatusCode: response.StatusCode,
			Header:     cloneHeader(response.Header),
			Body:       cloneBytes(body),
			Err:        ErrVerificationFailed,
		}
	}
	if !bytes.Equal(body, []byte(expectedBody)) {
		return fmt.Errorf("%w: challenge mismatch", ErrVerificationFailed)
	}
	return nil
}

func callSubscriptionValidation(callback SubscriptionValidationFunc, ctx context.Context, message Subscription, metadata RequestMetadata) (err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata)
}

func callSubscriptionVerified(callback SubscriptionVerifiedFunc, ctx context.Context, message VerifiedSubscription, metadata RequestMetadata) (err error) {
	defer recoverCallback(&err)
	return callback(ctx, cloneVerifiedSubscription(message), metadata)
}

func callUnsubscriptionValidation(callback UnsubscriptionValidationFunc, ctx context.Context, message Unsubscription, metadata RequestMetadata) (err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata)
}

func callUnsubscriptionVerified(callback UnsubscriptionVerifiedFunc, ctx context.Context, message VerifiedUnsubscription, metadata RequestMetadata) (err error) {
	defer recoverCallback(&err)
	return callback(ctx, cloneVerifiedUnsubscription(message), metadata)
}

func cloneVerifiedSubscription(message VerifiedSubscription) VerifiedSubscription {
	message.Subscription = cloneSubscription(message.Subscription)
	return message
}

func cloneVerifiedUnsubscription(message VerifiedUnsubscription) VerifiedUnsubscription {
	message.Unsubscription = cloneUnsubscription(message.Unsubscription)
	return message
}
