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
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"hash"
	"net/http"
	"time"
)

const (
	// HeaderHubSignature contains the WebSub content signature.
	HeaderHubSignature = "X-Hub-Signature"
)

// SignatureAlgorithm identifies an HMAC algorithm for authenticated content
// distribution.
type SignatureAlgorithm string

const (
	// SignatureSHA256 selects HMAC-SHA256 and is the default.
	SignatureSHA256 SignatureAlgorithm = "sha256"
	// SignatureSHA384 selects HMAC-SHA384.
	SignatureSHA384 SignatureAlgorithm = "sha384"
	// SignatureSHA512 selects HMAC-SHA512.
	SignatureSHA512 SignatureAlgorithm = "sha512"
)

// DeliveryConfig controls outbound delivery transport. Zero values select
// bounded package defaults.
type DeliveryConfig struct {
	// HTTPClient supplies transport settings. It is copied, never mutated, and
	// configured to refuse redirects.
	HTTPClient *http.Client
	// Timeout bounds one delivery attempt.
	Timeout time.Duration
	// MaxResponseBody bounds the subscriber response body in bytes.
	MaxResponseBody int64
	// SignatureAlgorithm selects the HMAC algorithm used when the subscription
	// has a secret. The zero value selects SignatureSHA256.
	SignatureAlgorithm SignatureAlgorithm
}

// ContentDistribution is one exact payload sent to one subscriber.
type ContentDistribution struct {
	// ContentType is the complete media type, including parameters.
	ContentType string
	// Body contains the exact bytes to deliver and sign.
	Body []byte
	// Header supplies additional delivery headers, subject to safety validation.
	Header http.Header
}

// DeliveryResponse is a bounded, detached snapshot of a subscriber response.
type DeliveryResponse struct {
	// StatusCode is the subscriber's HTTP response status.
	StatusCode int
	// Header is a detached copy of the response headers.
	Header http.Header
	// Body contains at most DeliveryConfig.MaxResponseBody bytes.
	Body []byte
}

// DeliveryClient sends content to a single immutable subscription.
type DeliveryClient struct {
	subscription       Subscription
	client             *http.Client
	maxBody            int64
	signatureAlgorithm SignatureAlgorithm
	signatureHash      func() hash.Hash
}

// NewDeliveryClient validates and snapshots a subscription and its transport.
// It does not mutate config.HTTPClient.
func NewDeliveryClient(subscription Subscription, config DeliveryConfig) (*DeliveryClient, error) {
	if _, err := validateHTTPURL(subscription.Hub, "Hub", true, true); err != nil {
		return nil, err
	}
	if _, err := validateHTTPURL(subscription.Topic, "Topic", false, false); err != nil {
		return nil, err
	}
	if _, err := validateHTTPURL(subscription.Callback, "Callback", true, true); err != nil {
		return nil, err
	}
	if len(subscription.Secret) > maxSecretBytes {
		return nil, invalidRequest("Secret must be shorter than 200 bytes")
	}
	if config.Timeout < 0 || config.MaxResponseBody < 0 {
		return nil, invalidRequest("delivery configuration values must not be negative")
	}
	algorithm, newHash, err := deliverySignature(config.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}
	if config.Timeout == 0 {
		config.Timeout = defaultClientTimeout
	}
	if config.MaxResponseBody == 0 {
		config.MaxResponseBody = defaultDeliveryBody
	}
	return &DeliveryClient{
		subscription:       subscription,
		client:             newHTTPClient(config.HTTPClient, config.Timeout),
		maxBody:            config.MaxResponseBody,
		signatureAlgorithm: algorithm,
		signatureHash:      newHash,
	}, nil
}

func deliverySignature(algorithm SignatureAlgorithm) (SignatureAlgorithm, func() hash.Hash, error) {
	switch algorithm {
	case "", SignatureSHA256:
		return SignatureSHA256, sha256.New, nil
	case SignatureSHA384:
		return SignatureSHA384, sha512.New384, nil
	case SignatureSHA512:
		return SignatureSHA512, sha512.New, nil
	default:
		return "", nil, invalidRequest("unsupported delivery signature algorithm")
	}
}

// Deliver performs exactly one HTTP delivery attempt. It returns a
// DeliveryResponse for HTTP responses, including non-success responses. HTTP
// 410 errors match both ErrDeliveryFailed and ErrSubscriptionGone.
func (c *DeliveryClient) Deliver(ctx context.Context, content ContentDistribution) (DeliveryResponse, error) {
	if c == nil {
		return DeliveryResponse{}, invalidRequest("DeliveryClient is nil")
	}
	if err := validateContentType(content.ContentType); err != nil {
		return DeliveryResponse{}, err
	}
	reserved := map[string]struct{}{
		"Content-Length":   {},
		"Content-Type":     {},
		"Host":             {},
		"Link":             {},
		HeaderHubSignature: {},
	}
	if err := validateHeaders(content.Header, reserved); err != nil {
		return DeliveryResponse{}, err
	}
	body := cloneBytes(content.Body)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.subscription.Callback, bytes.NewReader(body))
	if err != nil {
		return DeliveryResponse{}, errors.Join(ErrDeliveryFailed, err)
	}
	copyHeaders(request.Header, content.Header)
	request.Header.Set("Content-Type", content.ContentType)
	request.Header.Add("Link", encodedLink(c.subscription.Hub, "hub"))
	request.Header.Add("Link", encodedLink(c.subscription.Topic, "self"))
	if c.subscription.Secret != "" {
		mac := hmac.New(c.signatureHash, []byte(c.subscription.Secret))
		_, _ = mac.Write(body)
		request.Header.Set(HeaderHubSignature, string(c.signatureAlgorithm)+"="+hex.EncodeToString(mac.Sum(nil)))
	}
	response, err := c.client.Do(request)
	if err != nil {
		return DeliveryResponse{}, errors.Join(ErrDeliveryFailed, err)
	}
	responseBody, readErr := responseSnapshot(response, c.maxBody, "content delivery", ErrDeliveryFailed)
	snapshot := DeliveryResponse{
		StatusCode: response.StatusCode,
		Header:     cloneHeader(response.Header),
		Body:       cloneBytes(responseBody),
	}
	if readErr != nil {
		return snapshot, readErr
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return snapshot, nil
	}
	class := ErrDeliveryFailed
	if response.StatusCode == http.StatusGone {
		class = errors.Join(ErrDeliveryFailed, ErrSubscriptionGone)
	}
	return snapshot, &HTTPError{
		Operation:  "content delivery",
		StatusCode: response.StatusCode,
		Header:     cloneHeader(response.Header),
		Body:       cloneBytes(responseBody),
		Err:        class,
	}
}
