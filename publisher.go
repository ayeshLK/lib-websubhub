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
	"net/http"
	"net/url"
)

// HeaderGoPublisher identifies the publisher extension mode header.
const HeaderGoPublisher = publisherHeader

// PublisherClientConfig controls publisher extension requests.
type PublisherClientConfig struct {
	// HubURL is the absolute HTTP(S) endpoint of an extension-enabled hub.
	HubURL string
	// HTTPClient supplies transport settings. It is copied, never mutated, and
	// configured to refuse redirects.
	HTTPClient *http.Client
	// MaxResponseBody bounds publisher-operation response bodies in bytes.
	MaxResponseBody int64
}

// PublisherClient invokes the optional, non-standard publisher extension.
// It is safe for concurrent use after construction.
type PublisherClient struct {
	hubURL  string
	client  *http.Client
	maxBody int64
}

// RegisterTopicOption configures a topic-registration request. Implementations
// are provided by this package.
type RegisterTopicOption interface {
	applyRegisterTopic(*registerTopicOptions) error
}

type registerTopicOptions struct {
	contentType    string
	contentTypeSet bool
}

type registerTopicOptionFunc func(*registerTopicOptions) error

func (option registerTopicOptionFunc) applyRegisterTopic(options *registerTopicOptions) error {
	if option == nil {
		return invalidRequest("RegisterTopicOption must not be nil")
	}
	return option(options)
}

// WithTopicContentType declares the topic's expected complete media type.
func WithTopicContentType(contentType string) RegisterTopicOption {
	return registerTopicOptionFunc(func(options *registerTopicOptions) error {
		if options.contentTypeSet {
			return invalidRequest("topic content type must not be supplied more than once")
		}
		if err := validateContentType(contentType); err != nil {
			return err
		}
		options.contentType = contentType
		options.contentTypeSet = true
		return nil
	})
}

// NewPublisherClient validates and snapshots publisher transport settings.
// It does not mutate config.HTTPClient.
func NewPublisherClient(config PublisherClientConfig) (*PublisherClient, error) {
	if _, err := validateHTTPURL(config.HubURL, "HubURL", true, true); err != nil {
		return nil, err
	}
	if config.MaxResponseBody < 0 {
		return nil, invalidRequest("MaxResponseBody must not be negative")
	}
	if config.MaxResponseBody == 0 {
		config.MaxResponseBody = defaultDeliveryBody
	}
	return &PublisherClient{
		hubURL:  config.HubURL,
		client:  newHTTPClient(config.HTTPClient, defaultClientTimeout),
		maxBody: config.MaxResponseBody,
	}, nil
}

// RegisterTopic requests registration of an absolute HTTP(S) topic URL with
// optional publisher-extension metadata.
func (c *PublisherClient) RegisterTopic(ctx context.Context, topic string, supplied ...RegisterTopicOption) error {
	if c == nil {
		return invalidRequest("PublisherClient is nil")
	}
	options := registerTopicOptions{}
	for _, option := range supplied {
		if option == nil {
			return invalidRequest("RegisterTopicOption must not be nil")
		}
		if err := option.applyRegisterTopic(&options); err != nil {
			return err
		}
	}
	fields := url.Values{}
	if options.contentTypeSet {
		fields.Set("hub.content_type", options.contentType)
	}
	return c.sendForm(ctx, ModeRegister, topic, "", fields)
}

// DeregisterTopic requests deregistration of an absolute HTTP(S) topic URL.
func (c *PublisherClient) DeregisterTopic(ctx context.Context, topic string) error {
	return c.sendForm(ctx, ModeDeregister, topic, "", nil)
}

// Notify sends an event-only update notification.
func (c *PublisherClient) Notify(ctx context.Context, topic string) error {
	return c.sendForm(ctx, ModePublish, topic, "event", nil)
}

// Publish sends one exact content update.
func (c *PublisherClient) Publish(ctx context.Context, message UpdateMessage) error {
	if c == nil {
		return invalidRequest("PublisherClient is nil")
	}
	if _, err := validateHTTPURL(message.Topic, "Topic", false, false); err != nil {
		return err
	}
	if err := validateContentType(message.ContentType); err != nil {
		return err
	}
	reserved := map[string]struct{}{
		"Content-Length":  {},
		"Content-Type":    {},
		"Host":            {},
		HeaderGoPublisher: {},
	}
	if err := validateHeaders(message.Header, reserved); err != nil {
		return err
	}
	target, err := url.Parse(c.hubURL)
	if err != nil {
		return errors.Join(ErrPublisherFailed, err)
	}
	query := target.Query()
	query.Set("hub.mode", string(ModePublish))
	query.Set("hub.topic", message.Topic)
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(cloneBytes(message.Body)))
	if err != nil {
		return errors.Join(ErrPublisherFailed, err)
	}
	copyHeaders(request.Header, message.Header)
	request.Header.Set("Content-Type", message.ContentType)
	request.Header.Set(HeaderGoPublisher, "publish")
	return c.do(request)
}

func (c *PublisherClient) sendForm(ctx context.Context, mode Mode, topic, extensionMode string, fields url.Values) error {
	if c == nil {
		return invalidRequest("PublisherClient is nil")
	}
	if _, err := validateHTTPURL(topic, "Topic", false, false); err != nil {
		return err
	}
	form := url.Values{
		"hub.mode":  {string(mode)},
		"hub.topic": {topic},
	}
	for name, values := range fields {
		form[name] = append([]string(nil), values...)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.hubURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return errors.Join(ErrPublisherFailed, err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if extensionMode != "" {
		request.Header.Set(HeaderGoPublisher, extensionMode)
	}
	return c.do(request)
}

func (c *PublisherClient) do(request *http.Request) error {
	response, err := c.client.Do(request)
	if err != nil {
		return errors.Join(ErrPublisherFailed, err)
	}
	body, readErr := responseSnapshot(response, c.maxBody, "publisher request", ErrPublisherFailed)
	if readErr != nil {
		return readErr
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && formAccepted(body) {
		return nil
	}
	return &HTTPError{
		Operation:  "publisher request",
		StatusCode: response.StatusCode,
		Header:     cloneHeader(response.Header),
		Body:       cloneBytes(body),
		Err:        ErrPublisherFailed,
	}
}
