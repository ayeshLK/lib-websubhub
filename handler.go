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
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const publisherHeader = "X-Go-Publisher"

type verificationJob struct {
	ctx      context.Context
	start    chan struct{}
	mode     Mode
	sub      Subscription
	unsub    Unsubscription
	metadata RequestMetadata
	verified bool
}

// Handler adapts WebSub HTTP requests to application callbacks. It implements
// http.Handler and is safe for concurrent use.
type Handler struct {
	config  Config
	service Service
	client  *http.Client
	logger  *slog.Logger

	rootContext context.Context
	rootCancel  context.CancelFunc
	jobs        chan verificationJob

	admissionMu sync.Mutex
	closed      bool
	jobsWG      sync.WaitGroup
	workersWG   sync.WaitGroup
	closeOnce   sync.Once
	closeDone   chan struct{}
}

// NewHandler validates configuration and starts bounded verification workers.
// Service.OnSubscriptionVerified and Service.OnUnsubscriptionVerified are
// required. Publisher callbacks are also required when the publisher extension
// is enabled.
func NewHandler(config Config, service Service) (*Handler, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if service.OnSubscriptionVerified == nil {
		return nil, invalidRequest("OnSubscriptionVerified is required")
	}
	if service.OnUnsubscriptionVerified == nil {
		return nil, invalidRequest("OnUnsubscriptionVerified is required")
	}
	if normalized.EnablePublisherExtension {
		if service.OnRegisterTopic == nil || service.OnDeregisterTopic == nil || service.OnUpdateMessage == nil {
			return nil, invalidRequest("all publisher extension callbacks are required")
		}
	}

	rootContext, rootCancel := context.WithCancel(context.Background())
	h := &Handler{
		config:      normalized,
		service:     service,
		client:      newHTTPClient(normalized.HTTPClient, normalized.VerificationTimeout),
		logger:      normalized.Logger,
		rootContext: rootContext,
		rootCancel:  rootCancel,
		jobs:        make(chan verificationJob, normalized.VerificationQueue),
		closeDone:   make(chan struct{}),
	}
	for range normalized.VerificationWorkers {
		h.workersWG.Add(1)
		go h.worker()
	}
	return h, nil
}

func normalizeConfig(config Config) (Config, error) {
	if _, err := validateHTTPURL(config.HubURL, "HubURL", true, true); err != nil {
		return Config{}, err
	}
	if config.DefaultLease < 0 || config.MaxLease < 0 ||
		config.VerificationTimeout < 0 || config.MaxRequestBody < 0 ||
		config.MaxCallbackBody < 0 || config.VerificationWorkers < 0 ||
		config.VerificationQueue < 0 {
		return Config{}, invalidRequest("configuration values must not be negative")
	}
	if config.DefaultLease == 0 {
		config.DefaultLease = defaultLease
	}
	if config.MaxLease == 0 {
		config.MaxLease = defaultLease
	}
	if config.DefaultLease < time.Second || config.MaxLease < time.Second ||
		config.DefaultLease%time.Second != 0 || config.MaxLease%time.Second != 0 {
		return Config{}, invalidRequest("lease configuration must use positive whole seconds")
	}
	if config.DefaultLease > config.MaxLease {
		return Config{}, invalidRequest("DefaultLease must not exceed MaxLease")
	}
	if config.MaxRequestBody == 0 {
		config.MaxRequestBody = defaultRequestBody
	}
	if config.MaxCallbackBody == 0 {
		config.MaxCallbackBody = defaultCallbackBody
	}
	if config.VerificationTimeout == 0 {
		config.VerificationTimeout = defaultVerificationTimeout
	}
	if config.VerificationWorkers == 0 {
		config.VerificationWorkers = defaultWorkers
	}
	if config.VerificationQueue == 0 {
		config.VerificationQueue = defaultQueue
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return config, nil
}

// ServeHTTP parses and dispatches one hub request. Successful subscription and
// unsubscription admission returns HTTP 202 before asynchronous verification
// completes.
func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.isClosed() {
		http.Error(response, ErrClosed.Error(), http.StatusServiceUnavailable)
		return
	}

	contentType := request.Header.Get("Content-Type")
	mediaType, err := parseMediaType(contentType)
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	if mediaType == "application/x-www-form-urlencoded" {
		if err = validateFormContentType(contentType); err != nil {
			h.writeSafeError(response, http.StatusBadRequest)
			return
		}
	}

	publisherValues := request.Header.Values(publisherHeader)
	if len(publisherValues) > 1 {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	publisherMode := ""
	if len(publisherValues) == 1 {
		publisherMode = strings.ToLower(strings.TrimSpace(publisherValues[0]))
	}

	if publisherMode == "event" {
		if !h.config.EnablePublisherExtension || mediaType != "application/x-www-form-urlencoded" {
			h.writeSafeError(response, http.StatusBadRequest)
			return
		}
		body, readErr := h.readRequestBody(response, request)
		if readErr != nil {
			h.writeSafeError(response, http.StatusBadRequest)
			return
		}
		values, parseErr := url.ParseQuery(string(body))
		if parseErr != nil || values.Get("hub.mode") != string(ModePublish) || len(values["hub.mode"]) != 1 {
			h.writeSafeError(response, http.StatusBadRequest)
			return
		}
		h.serveEvent(response, request, values, contentType)
		return
	}

	if publisherMode != "" || mediaType != "application/x-www-form-urlencoded" {
		if err := h.servePublisherContent(response, request, contentType, publisherMode); err != nil {
			h.writeSafeError(response, http.StatusBadRequest)
		}
		return
	}

	body, err := h.readRequestBody(response, request)
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	modeValue, err := requiredSingle(values, "hub.mode")
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}

	switch Mode(modeValue) {
	case ModeSubscribe:
		h.serveSubscription(response, request, values)
	case ModeUnsubscribe:
		h.serveUnsubscription(response, request, values)
	case ModeRegister:
		h.serveRegistration(response, request, values, false)
	case ModeDeregister:
		h.serveRegistration(response, request, values, true)
	case ModePublish:
		h.writeSafeError(response, http.StatusBadRequest)
	default:
		h.writeSafeError(response, http.StatusBadRequest)
	}
}

func (h *Handler) serveSubscription(response http.ResponseWriter, request *http.Request, values url.Values) {
	message, err := h.parseSubscription(values)
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	metadata := metadataFromRequest(request)
	controller := &Controller{allowed: h.config.AllowExternalVerification}
	result := Result{}
	if callback := h.service.OnSubscription; callback != nil {
		result, err = callSubscription(callback, request.Context(), cloneSubscription(message), cloneMetadata(metadata), controller)
	}
	if h.handleInitialError(response, err, true) {
		return
	}
	result = cloneResult(result)
	accepted, resultErr := validateAdmissionResult(result)
	if resultErr != nil {
		h.writeSafeError(response, http.StatusInternalServerError)
		return
	}
	if !accepted {
		h.writeResult(response, result, http.StatusBadRequest, nil)
		return
	}
	result.StatusCode = 0

	job := verificationJob{
		ctx:      context.WithoutCancel(request.Context()),
		start:    make(chan struct{}),
		mode:     ModeSubscribe,
		sub:      cloneSubscription(message),
		metadata: cloneMetadata(metadata),
		verified: controller.isMarked(),
	}
	if !h.admit(job) {
		h.writeSafeError(response, http.StatusServiceUnavailable)
		return
	}
	h.writeResult(response, result, http.StatusAccepted, nil)
	close(job.start)
}

func (h *Handler) serveUnsubscription(response http.ResponseWriter, request *http.Request, values url.Values) {
	message, err := h.parseUnsubscription(values)
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	metadata := metadataFromRequest(request)
	controller := &Controller{allowed: h.config.AllowExternalVerification}
	result := Result{}
	if callback := h.service.OnUnsubscription; callback != nil {
		result, err = callUnsubscription(callback, request.Context(), cloneUnsubscription(message), cloneMetadata(metadata), controller)
	}
	if h.handleInitialError(response, err, true) {
		return
	}
	result = cloneResult(result)
	accepted, resultErr := validateAdmissionResult(result)
	if resultErr != nil {
		h.writeSafeError(response, http.StatusInternalServerError)
		return
	}
	if !accepted {
		h.writeResult(response, result, http.StatusBadRequest, nil)
		return
	}
	result.StatusCode = 0

	job := verificationJob{
		ctx:      context.WithoutCancel(request.Context()),
		start:    make(chan struct{}),
		mode:     ModeUnsubscribe,
		unsub:    cloneUnsubscription(message),
		metadata: cloneMetadata(metadata),
		verified: controller.isMarked(),
	}
	if !h.admit(job) {
		h.writeSafeError(response, http.StatusServiceUnavailable)
		return
	}
	h.writeResult(response, result, http.StatusAccepted, nil)
	close(job.start)
}

func (h *Handler) serveRegistration(response http.ResponseWriter, request *http.Request, values url.Values, deregister bool) {
	if !h.config.EnablePublisherExtension {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	topic, err := requiredSingle(values, "hub.topic")
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	if topic, err = normalizeHTTPURL(topic, "hub.topic", false, false); err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	metadata := metadataFromRequest(request)
	var result Result
	if deregister {
		result, err = callDeregister(h.service.OnDeregisterTopic, request.Context(), TopicDeregistration{
			Mode: ModeDeregister, Topic: topic,
		}, cloneMetadata(metadata))
	} else {
		result, err = callRegister(h.service.OnRegisterTopic, request.Context(), TopicRegistration{
			Mode: ModeRegister, Topic: topic,
		}, cloneMetadata(metadata))
	}
	if err != nil {
		h.writeExtensionDenied(response, err)
		return
	}
	result = cloneResult(result)
	if validateResult(result) != nil {
		h.writeSafeError(response, http.StatusInternalServerError)
		return
	}
	defaultBody := []byte(url.Values{"hub.mode": {"accepted"}}.Encode())
	h.writeResult(response, result, http.StatusOK, defaultBody)
}

func (h *Handler) serveEvent(response http.ResponseWriter, request *http.Request, values url.Values, contentType string) {
	topic, err := requiredSingle(values, "hub.topic")
	if err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	if topic, err = normalizeHTTPURL(topic, "hub.topic", false, false); err != nil {
		h.writeSafeError(response, http.StatusBadRequest)
		return
	}
	message := UpdateMessage{
		Kind:        UpdateEvent,
		Topic:       topic,
		ContentType: contentType,
		Header:      cloneHeader(request.Header),
	}
	h.serveUpdate(response, request, message)
}

func (h *Handler) servePublisherContent(response http.ResponseWriter, request *http.Request, contentType, publisherMode string) error {
	if !h.config.EnablePublisherExtension {
		return invalidRequest("publisher extension is disabled")
	}
	if publisherMode != "" && publisherMode != "publish" {
		return invalidRequest("invalid publisher header")
	}
	mode, err := requiredSingle(request.URL.Query(), "hub.mode")
	if err != nil || mode != string(ModePublish) {
		return invalidRequest("invalid publish mode")
	}
	topic, err := requiredSingle(request.URL.Query(), "hub.topic")
	if err != nil {
		return err
	}
	if topic, err = normalizeHTTPURL(topic, "hub.topic", false, false); err != nil {
		return err
	}
	body, err := h.readRequestBody(response, request)
	if err != nil {
		return err
	}
	message := UpdateMessage{
		Kind:        UpdateContent,
		Topic:       topic,
		ContentType: contentType,
		Body:        cloneBytes(body),
		Header:      cloneHeader(request.Header),
	}
	h.serveUpdate(response, request, message)
	return nil
}

func (h *Handler) serveUpdate(response http.ResponseWriter, request *http.Request, message UpdateMessage) {
	result, err := callUpdate(h.service.OnUpdateMessage, request.Context(), cloneUpdate(message), metadataFromRequest(request))
	if err != nil {
		h.writeExtensionDenied(response, err)
		return
	}
	result = cloneResult(result)
	if validateResult(result) != nil {
		h.writeSafeError(response, http.StatusInternalServerError)
		return
	}
	defaultBody := []byte(url.Values{"hub.mode": {"accepted"}}.Encode())
	h.writeResult(response, result, http.StatusAccepted, defaultBody)
}

func (h *Handler) readRequestBody(response http.ResponseWriter, request *http.Request) ([]byte, error) {
	request.Body = http.MaxBytesReader(response, request.Body, h.config.MaxRequestBody)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (h *Handler) handleInitialError(response http.ResponseWriter, err error, redirectAllowed bool) bool {
	if err == nil {
		return false
	}
	var redirect *RedirectError
	if errors.As(err, &redirect) {
		if !redirectAllowed || (redirect.StatusCode != http.StatusTemporaryRedirect &&
			redirect.StatusCode != http.StatusPermanentRedirect) {
			h.writeSafeError(response, http.StatusInternalServerError)
			return true
		}
		if _, parseErr := validateHTTPURL(redirect.Location, "redirect location", true, true); parseErr != nil {
			h.writeSafeError(response, http.StatusInternalServerError)
			return true
		}
		response.Header().Set("Location", redirect.Location)
		response.WriteHeader(redirect.StatusCode)
		return true
	}
	if errors.Is(err, ErrDenied) {
		h.writeSafeError(response, http.StatusBadRequest)
		return true
	}
	h.writeSafeError(response, http.StatusInternalServerError)
	return true
}

func (h *Handler) writeExtensionDenied(response http.ResponseWriter, err error) {
	reason := "operation denied"
	var denied *DeniedError
	if errors.As(err, &denied) && denied.Reason != "" {
		reason = safeReason(denied.Reason)
	}
	body := []byte(url.Values{
		"hub.mode":   {"denied"},
		"hub.reason": {reason},
	}.Encode())
	result := Result{
		StatusCode:  http.StatusBadRequest,
		ContentType: "application/x-www-form-urlencoded",
		Body:        body,
	}
	h.writeResult(response, result, http.StatusBadRequest, nil)
}

func (h *Handler) writeSafeError(response http.ResponseWriter, status int) {
	http.Error(response, http.StatusText(status), status)
}

func (h *Handler) writeResult(response http.ResponseWriter, result Result, defaultStatus int, defaultBody []byte) {
	status := result.StatusCode
	if status == 0 {
		status = defaultStatus
	}
	body := result.Body
	if body == nil {
		body = defaultBody
	}
	copyHeaders(response.Header(), result.Header)
	contentType := result.ContentType
	if contentType == "" && len(body) > 0 {
		contentType = "application/x-www-form-urlencoded"
	}
	if contentType != "" {
		response.Header().Set("Content-Type", contentType)
	}
	response.WriteHeader(status)
	if len(body) > 0 {
		_, _ = response.Write(body)
	}
}

func validateResult(result Result) error {
	if result.StatusCode != 0 && (result.StatusCode < 200 || result.StatusCode > 599) {
		return invalidRequest("invalid result status")
	}
	reserved := map[string]struct{}{"Content-Length": {}, "Transfer-Encoding": {}}
	if result.ContentType != "" {
		reserved["Content-Type"] = struct{}{}
	}
	if err := validateHeaders(result.Header, reserved); err != nil {
		return err
	}
	if result.ContentType != "" {
		if err := validateContentType(result.ContentType); err != nil {
			return err
		}
	}
	return nil
}

func validateAdmissionResult(result Result) (bool, error) {
	if err := validateResult(result); err != nil {
		return false, err
	}
	switch {
	case result.StatusCode == 0 || result.StatusCode == http.StatusAccepted:
		return true, nil
	case result.StatusCode >= 400 && result.StatusCode <= 599:
		return false, nil
	default:
		return false, invalidRequest("subscription success status must be 202")
	}
}

func requiredSingle(values url.Values, key string) (string, error) {
	items, ok := values[key]
	if !ok || len(items) != 1 || items[0] == "" {
		return "", invalidRequest(key + " must occur exactly once and be non-empty")
	}
	return items[0], nil
}

func unknownParameters(values url.Values, reserved ...string) url.Values {
	out := cloneValues(values)
	for _, key := range reserved {
		delete(out, key)
	}
	return out
}

func (h *Handler) parseSubscription(values url.Values) (Subscription, error) {
	topic, err := requiredSingle(values, "hub.topic")
	if err != nil {
		return Subscription{}, err
	}
	callback, err := requiredSingle(values, "hub.callback")
	if err != nil {
		return Subscription{}, err
	}
	if topic, err = normalizeHTTPURL(topic, "hub.topic", false, false); err != nil {
		return Subscription{}, err
	}
	if callback, err = normalizeHTTPURL(callback, "hub.callback", true, true); err != nil {
		return Subscription{}, err
	}

	leaseSeconds := ""
	if raw, exists := values["hub.lease_seconds"]; exists {
		if len(raw) != 1 || raw[0] == "" {
			return Subscription{}, invalidRequest("hub.lease_seconds must occur once")
		}
		seconds, parseErr := strconv.ParseInt(raw[0], 10, 64)
		if parseErr != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
			return Subscription{}, invalidRequest("hub.lease_seconds must be a positive integer")
		}
		leaseSeconds = raw[0]
	}
	secret, err := parseSecret(values)
	if err != nil {
		return Subscription{}, err
	}
	return Subscription{
		Hub:          h.config.HubURL,
		Mode:         ModeSubscribe,
		Topic:        topic,
		Callback:     callback,
		LeaseSeconds: leaseSeconds,
		Secret:       secret,
		Parameters: unknownParameters(values, "hub.mode", "hub.topic", "hub.callback",
			"hub.lease_seconds", "hub.secret"),
	}, nil
}

func (h *Handler) parseUnsubscription(values url.Values) (Unsubscription, error) {
	topic, err := requiredSingle(values, "hub.topic")
	if err != nil {
		return Unsubscription{}, err
	}
	callback, err := requiredSingle(values, "hub.callback")
	if err != nil {
		return Unsubscription{}, err
	}
	if topic, err = normalizeHTTPURL(topic, "hub.topic", false, false); err != nil {
		return Unsubscription{}, err
	}
	if callback, err = normalizeHTTPURL(callback, "hub.callback", true, true); err != nil {
		return Unsubscription{}, err
	}
	secret, err := parseSecret(values)
	if err != nil {
		return Unsubscription{}, err
	}
	return Unsubscription{
		Mode:     ModeUnsubscribe,
		Topic:    topic,
		Callback: callback,
		Secret:   secret,
		Parameters: unknownParameters(values, "hub.mode", "hub.topic", "hub.callback",
			"hub.lease_seconds", "hub.secret"),
	}, nil
}

func parseSecret(values url.Values) (string, error) {
	raw, exists := values["hub.secret"]
	if !exists {
		return "", nil
	}
	if len(raw) != 1 {
		return "", invalidRequest("hub.secret must not be duplicated")
	}
	secret := raw[0]
	if secret == "" {
		return "", invalidRequest("hub.secret must not be empty")
	}
	if len(secret) > maxSecretBytes {
		return "", invalidRequest("hub.secret must be shorter than 200 bytes")
	}
	return secret, nil
}

func metadataFromRequest(request *http.Request) RequestMetadata {
	return RequestMetadata{Header: cloneHeader(request.Header), RemoteAddr: request.RemoteAddr}
}

func cloneMetadata(metadata RequestMetadata) RequestMetadata {
	return RequestMetadata{Header: cloneHeader(metadata.Header), RemoteAddr: metadata.RemoteAddr}
}

func cloneSubscription(message Subscription) Subscription {
	message.Parameters = cloneValues(message.Parameters)
	return message
}

func cloneUnsubscription(message Unsubscription) Unsubscription {
	message.Parameters = cloneValues(message.Parameters)
	return message
}

func cloneUpdate(message UpdateMessage) UpdateMessage {
	message.Body = cloneBytes(message.Body)
	message.Header = cloneHeader(message.Header)
	return message
}

func cloneResult(result Result) Result {
	result.Header = cloneHeader(result.Header)
	result.Body = cloneBytes(result.Body)
	return result
}

func (h *Handler) isClosed() bool {
	h.admissionMu.Lock()
	defer h.admissionMu.Unlock()
	return h.closed
}

func (h *Handler) admit(job verificationJob) bool {
	h.admissionMu.Lock()
	defer h.admissionMu.Unlock()
	if h.closed {
		return false
	}
	h.jobsWG.Add(1)
	select {
	case h.jobs <- job:
		return true
	default:
		h.jobsWG.Done()
		return false
	}
}

func (h *Handler) worker() {
	defer h.workersWG.Done()
	for {
		select {
		case <-h.rootContext.Done():
			return
		case job := <-h.jobs:
			h.runJob(job)
			h.jobsWG.Done()
		}
	}
}

func (h *Handler) runJob(job verificationJob) {
	select {
	case <-job.start:
	case <-h.rootContext.Done():
		return
	}
	ctx, cancel := context.WithTimeout(job.ctx, h.config.VerificationTimeout)
	stop := context.AfterFunc(h.rootContext, cancel)
	defer func() {
		stop()
		cancel()
	}()
	switch job.mode {
	case ModeSubscribe:
		h.runSubscription(ctx, job)
	case ModeUnsubscribe:
		h.runUnsubscription(ctx, job)
	}
}

// Close stops accepting requests and waits for admitted verification work.
// When ctx expires, it cancels remaining work and returns ctx.Err. Close is
// safe to call repeatedly.
func (h *Handler) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		h.admissionMu.Lock()
		h.closed = true
		h.admissionMu.Unlock()
		go func() {
			h.jobsWG.Wait()
			h.rootCancel()
			h.workersWG.Wait()
			close(h.closeDone)
		}()
	})

	select {
	case <-h.closeDone:
		return nil
	case <-ctx.Done():
		h.rootCancel()
		// Admission is closed before closeDone is started, so no new jobs can
		// enter the queue. Workers may exit as soon as rootContext is canceled;
		// account for any jobs they leave queued so the background shutdown can
		// finish and a later Close call does not wait forever.
		for {
			select {
			case <-h.jobs:
				h.jobsWG.Done()
			default:
				return ctx.Err()
			}
		}
	}
}

func callSubscription(callback SubscriptionFunc, ctx context.Context, message Subscription, metadata RequestMetadata, controller *Controller) (result Result, err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata, controller)
}

func callUnsubscription(callback UnsubscriptionFunc, ctx context.Context, message Unsubscription, metadata RequestMetadata, controller *Controller) (result Result, err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata, controller)
}

func callRegister(callback RegisterTopicFunc, ctx context.Context, message TopicRegistration, metadata RequestMetadata) (result Result, err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata)
}

func callDeregister(callback DeregisterTopicFunc, ctx context.Context, message TopicDeregistration, metadata RequestMetadata) (result Result, err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata)
}

func callUpdate(callback UpdateMessageFunc, ctx context.Context, message UpdateMessage, metadata RequestMetadata) (result Result, err error) {
	defer recoverCallback(&err)
	return callback(ctx, message, metadata)
}

func recoverCallback(err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("websubhub: application callback panicked")
	}
}
