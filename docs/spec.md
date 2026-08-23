# WebSubHub Framework Specification

Main Branch Edition — August 2026

- Implementation baseline: Go sources in the repository root at this revision
- Language baseline: Go 1.25 or newer
- Dependency baseline: Go standard library only
- Protocol baseline: [W3C WebSub Recommendation, 2 June 2026](https://www.w3.org/TR/websub/)

This document specifies how `github.com/ayeshLK/lib-websubhub` is constructed
and how its framework-owned behavior executes. It is written for maintainers and
implementers of the package. Application developers should begin with the
[README](../README.md), Go API documentation, and examples.

## 1. Overview

`websubhub` is a thin, composable WebSub hub protocol framework for Go. It
adapts one HTTP endpoint to typed application callbacks, coordinates
subscription intent verification, and provides focused clients for publisher
operations and one-subscriber content delivery.

The framework is designed around five principles:

1. **Protocol mechanics are separate from application state.** The package
   parses, validates, verifies, and maps HTTP. It does not store topics or
   subscriptions.
2. **Observable work is bounded.** Request bodies, callback bodies, queues,
   workers, timeouts, and shutdown are finite.
3. **Application handoffs are typed.** Go function types and constructor
   validation enforce callback shape and required combinations.
4. **Wire data is preserved where the protocol requires it.** Callback query
   bytes, payload bytes, content types, and signatures have explicit handling.
5. **Ordinary Go composition remains available.** The handler implements
   `http.Handler`; middleware, `http.Server`, `http.Client`, contexts, and TLS
   retain their standard meanings.

The package is not a complete hub product or subscriber implementation. A
deployed hub must add persistence, lease expiry, fan-out, retry, authorization,
and operational controls.

## 2. Conformance and notation

### 2.1 Normative language

The key words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, MAY, and OPTIONAL are
interpreted as described by
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174).

All sections are normative unless marked **Note** or **Example**. Examples
illustrate behavior but do not extend the contract.

A conforming implementation MUST satisfy the observable requirements in this
document. Algorithms MAY be reorganized or optimized when their externally
observable HTTP behavior, callback order, error classification, concurrency,
and lifecycle are equivalent.

### 2.2 Terms

- **Framework** means the `websubhub` package.
- **Application** means code that constructs the framework and supplies
  callbacks, storage, workers, middleware, and deployment policy.
- **Admission callback** means the synchronous subscription or unsubscription
  callback invoked before HTTP acceptance.
- **Validation callback** means the optional asynchronous application check
  performed after acceptance and before intent verification.
- **Verified callback** means the required application handoff invoked only
  after successful intent verification.
- **Publisher extension** means the project-specific register, deregister,
  content publication, and event notification protocol. It is not standardized
  by WebSub.
- **Detached value** means a map, slice, header, byte slice, or form value whose
  mutable storage is not shared with the caller or inbound request.

### 2.3 Framework conformance and WebSub conformance

Conformance to this document means matching this Go framework contract. It does
not by itself establish W3C hub or subscriber conformance. Section 15 identifies
the WebSub behavior implemented by the framework and the behavior that a
complete application must supply.

## 3. Architecture and ownership

### 3.1 Component model

```text
Inbound HTTP
    -> Handler
        -> bounded parsing and validation
        -> synchronous admission callback
        -> exact HTTP response mapping
        -> bounded asynchronous validation
        -> subscriber intent verification
        -> verified callback

Application delivery worker
    -> DeliveryClient
        -> one subscriber callback

PublisherClient
    -> extension-enabled Handler

Publisher HTTP response
    -> AddDiscoveryLinks
```

The framework MUST NOT infer application state from callback results. Only
explicit typed callbacks cross the framework/application boundary.

### 3.2 Framework-owned behavior

The framework owns:

- HTTP method, media type, form, URL, lease, secret, and header validation;
- mode dispatch and safe HTTP response mapping;
- bounded verification admission and worker lifecycle;
- challenge generation and subscriber callback verification;
- detached callback inputs and panic containment;
- one delivery attempt to one subscription;
- discovery Link-header construction;
- the optional publisher extension client and handler protocol;
- typed sentinel and HTTP errors.

### 3.3 Application-owned behavior

The application owns:

- topic and verified-subscription persistence;
- atomic renewal and unsubscription;
- mandatory lease expiry;
- content selection, fan-out, ordering, retry, acknowledgement, and dead-letter
  policy;
- brokers, databases, reconciliation, clustering, replication, and node
  ownership;
- inbound authentication and authorization;
- callback and topic SSRF policy enforced at dial time;
- TLS termination, outbound trust, quotas, rate limiting, observability, and
  deployment lifecycle.

Storage, broker, scheduler, authentication, TLS, and telemetry abstractions MUST
NOT be added to the core package without deliberately revising this ownership
boundary.

## 4. Go package design

### 4.1 Module and package

The module path is `github.com/ayeshLK/lib-websubhub` and the root package name
is `websubhub`. The core module and its tests use only the Go standard library.

The implementation is divided by responsibility:

| File | Responsibility |
|---|---|
| `handler.go` | construction, inbound dispatch, responses, queueing, shutdown |
| `service.go` | public modes, messages, callbacks, configuration, controller |
| `subscription.go` | asynchronous validation and intent-verification execution |
| `delivery.go` | one-attempt subscriber delivery |
| `publisher.go` | publisher extension client |
| `discovery.go` | discovery Link headers |
| `helpers.go` | validation, copying, URL normalization, HTTP helpers |
| `errors.go` | sentinel and typed errors |
| `challenge.go` | cryptographically random challenges |

Unexported organization MAY change without affecting conformance.

### 4.2 Callback representation

Application operations are Go function types stored in a `Service` value rather
than methods on a large interface. This permits optional callbacks while normal
Go assignment checks callback signatures.

Every callback MAY be invoked concurrently. A framework implementation MUST:

- invoke callbacks with the provided `context.Context`;
- pass detached mutable values;
- recover callback panics at the framework boundary;
- convert a panic to a generic error that does not expose the panic value;
- never pass the live `http.ResponseWriter` or request body to application
  callbacks.

### 4.3 Required callbacks

`Service.OnSubscriptionVerified` and
`Service.OnUnsubscriptionVerified` are REQUIRED.

When `Config.EnablePublisherExtension` is true,
`OnRegisterTopic`, `OnDeregisterTopic`, and `OnUpdateMessage` are REQUIRED.
They MAY be nil while the extension is disabled.

Admission and validation callbacks are OPTIONAL. A missing admission callback
accepts a valid request. A missing validation callback allows verification to
continue.

### 4.4 Public API evolution

The exported Go declarations and this specification MUST describe the same
contract. A deliberate API or semantic change requires coordinated
implementation, tests, Go documentation, README, specification, and changelog
updates.

No compiler plugin, reflection-based callback discovery, code generator, or
custom analyzer is required. Go type checking and `NewHandler` validation enforce
the current structural invariants.

## 5. Protocol data model

### 5.1 Modes

`Mode` is a string with these defined values:

| Constant | Wire value | Meaning |
|---|---|---|
| `ModeSubscribe` | `subscribe` | WebSub subscription |
| `ModeUnsubscribe` | `unsubscribe` | WebSub unsubscription |
| `ModeRegister` | `register` | extension topic registration |
| `ModeDeregister` | `deregister` | extension topic deregistration |
| `ModePublish` | `publish` | extension update |

`UpdateKind` distinguishes `UpdateEvent` from `UpdateContent`. It is an
in-process discriminator and has no independent wire representation.

### 5.2 Subscription values

A `Subscription` contains:

- configured hub URL;
- mode `subscribe`;
- normalized callback and topic URLs;
- original requested lease string, or an empty string when omitted;
- optional secret;
- detached unknown form parameters.

A `VerifiedSubscription` anonymously contains that subscription and adds:

- `EffectiveLeaseSeconds`, the selected positive decimal lease;
- `LeaseStartedAt`, the time at which verification processing selected the
  effective lease.

An `Unsubscription` contains mode, normalized callback and topic, optional
secret, and detached unknown parameters. It does not expose a lease field.
Submitted `hub.lease_seconds` values are accepted and ignored for
unsubscription, including malformed or duplicated values.

A `VerifiedUnsubscription` contains the unsubscription after successful intent
verification.

### 5.3 Publisher and delivery values

`TopicRegistration` and `TopicDeregistration` contain only their fixed mode and
normalized topic.

`UpdateMessage` contains its kind, normalized topic, complete content type,
exact body bytes, and detached request headers. Event notifications have
`UpdateEvent` and no content body. Content publications have `UpdateContent`.

`ContentDistribution` contains one complete content type, exact body bytes, and
detached custom headers for one subscriber delivery. `DeliveryResponse`
contains the subscriber status, detached headers, and bounded response body.

### 5.4 Request metadata

`RequestMetadata` contains a detached `http.Header` and the inbound
`RemoteAddr`. Authentication middleware MAY attach values to the request
context; asynchronous callbacks retain those context values as described in
Section 13.

Headers may contain credentials or capability information. The framework MUST
not log metadata values.

### 5.5 Result values

`Result` controls a synchronous handler response:

- `StatusCode` zero selects the operation default;
- `Header` contains additional response headers;
- `ContentType` contains a complete media type;
- `Body` contains exact response bytes.

The framework MUST detach `Header` and `Body` before using them after a callback
returns.

### 5.6 Error model

The package defines these classifications:

| Error | Meaning |
|---|---|
| `ErrInvalidRequest` | invalid input or configuration |
| `ErrDenied` | application denial |
| `ErrVerificationFailed` | subscriber verification failure |
| `ErrSubscriptionGone` | subscriber returned HTTP 410 |
| `ErrDeliveryFailed` | content delivery failure |
| `ErrPublisherFailed` | publisher extension client failure |
| `ErrQueueFull` | verification capacity exhausted |
| `ErrClosed` | handler admission is closed |
| `ErrExternalVerificationDisabled` | disabled external verification |
| `ErrAlreadyVerified` | repeated external-verification mark |

`HTTPError` carries a safe operation name, status code, detached headers,
bounded body, and wrapped classification. `DeniedError` carries an optional
subscriber-safe reason and application cause. `RedirectError` carries an HTTP
307 or 308 status and redirect location.

Errors MUST support normal `errors.Is` and `errors.As` inspection.
Framework-generated protocol and remote-response errors MUST NOT include
secrets, authorization values, or payload bodies. Returned `net/http` transport
errors may include destination details; applications MUST treat them as
sensitive and apply log redaction.

## 6. Configuration and construction

### 6.1 Handler configuration

`Config.HubURL` is REQUIRED and MUST be an absolute HTTP or HTTPS URL without
userinfo or a fragment.

Zero values select these defaults:

| Field | Default |
|---|---|
| `DefaultLease` | 10 days |
| `MaxLease` | 10 days |
| `MaxRequestBody` | 64 KiB |
| `MaxCallbackBody` | 4 KiB |
| `VerificationTimeout` | 10 seconds |
| `VerificationWorkers` | 4 |
| `VerificationQueue` | 1,024 |
| `HTTPClient` | package client using a clone of `http.DefaultTransport` |
| `Logger` | discard logger |
| `AllowExternalVerification` | false |
| `EnablePublisherExtension` | false |
| `EnableHubErrorCallback` | false |

Durations, counts, and body limits MUST NOT be negative. Default and maximum
leases MUST be positive whole-second durations, and the default MUST NOT exceed
the maximum.

### 6.2 Handler construction algorithm

`NewHandler(config, service)` performs these steps:

1. Validate and apply configuration defaults.
2. Require both verified callbacks.
3. If the publisher extension is enabled, require all three publisher
   callbacks.
4. Create a handler-owned cancellation context.
5. Copy the supplied `http.Client` value, if any.
6. Install redirect refusal on the client copy.
7. Apply `VerificationTimeout` as the client timeout only when the supplied
   client has no timeout.
8. Allocate the bounded verification queue.
9. Start exactly `VerificationWorkers` workers.
10. Return the handler.

Construction failure MUST NOT return a partially usable handler. The supplied
`http.Client` and its fields MUST NOT be mutated.

### 6.3 Delivery and publisher clients

`DeliveryConfig` defaults to a 30-second timeout, a 64 KiB subscriber response
limit, and HMAC-SHA256 content signatures. It accepts `SignatureSHA256`,
`SignatureSHA384`, and `SignatureSHA512`; any other signature algorithm is
invalid. Negative values are invalid.

`PublisherClientConfig.HubURL` is REQUIRED and rejects userinfo and fragments.
Its response limit defaults to 64 KiB. The publisher client uses the same
copy-and-refuse-redirect client policy and a 30-second default timeout.

Constructed handlers and clients are safe for concurrent use.

## 7. Common validation and copying

### 7.1 Body bounds

Inbound handler bodies MUST be limited with `http.MaxBytesReader` before
`io.ReadAll`. Outbound response bodies MUST be read through a limit-plus-one
strategy so an oversized response is distinguishable from an exactly bounded
response. Every outbound response body MUST be closed.

### 7.2 Media types

Every inbound handler request requires a syntactically valid `Content-Type`
parsed with `mime.ParseMediaType`.

Form operations require `application/x-www-form-urlencoded`. An omitted
`charset` is accepted. An explicit charset is accepted only when it is UTF-8,
case-insensitively.

Publisher content and delivery content types may be any syntactically valid
media type. Their complete caller-supplied value, including parameters, MUST be
preserved.

### 7.3 Form fields

`hub.mode`, `hub.topic`, and `hub.callback` are required where defined. A
required field MUST occur exactly once and MUST be non-empty.

Unknown subscription and unsubscription parameters MUST be preserved in a
detached `url.Values` after reserved fields are removed. Registration and update
messages do not retain unknown parameters.

Malformed form percent encoding is invalid.

### 7.4 URL validation and normalization

Hub, topic, and callback values MUST be absolute HTTP or HTTPS URLs with a
non-empty host.

Hub and callback URLs reject userinfo and fragments. Topic URLs are validated as
absolute HTTP(S) URLs but the current implementation does not separately reject
userinfo or fragments.

After form or query decoding, inbound topic and callback strings are normalized
by decoding percent-encoded ASCII unreserved bytes:

```text
ALPHA / DIGIT / "-" / "." / "_" / "~"
```

Reserved escapes, non-ASCII escapes, escape case, ordering, duplicate query
keys, empty query values, and all other bytes remain unchanged.

URL validation is syntactic. It MUST NOT be represented as SSRF protection.

### 7.5 Lease and secret validation

A subscription lease, when present, MUST occur once and be a positive base-10
integer that can be represented as a Go `time.Duration` in seconds. The original
string is retained.

An omitted lease uses `DefaultLease`. A requested lease above `MaxLease` is
clamped. `EffectiveLeaseSeconds` is the selected duration expressed as base-10
whole seconds.

A secret MAY be omitted. If present, it MUST occur once, MUST be non-empty, and
MUST contain fewer than 200 bytes. The framework does not generate, assess, or
rotate secrets.

### 7.6 Header safety

Application-supplied headers MUST have valid names and MUST NOT contain carriage
return or line feed characters in values. These hop-by-hop fields are forbidden:

- `Connection`;
- `Keep-Alive`;
- `Proxy-Authenticate`;
- `Proxy-Authorization`;
- `TE`;
- `Trailer`;
- `Transfer-Encoding`;
- `Upgrade`.

Each operation additionally reserves headers it owns. Delivery reserves
`Content-Length`, `Content-Type`, `Host`, `Link`, and `X-Hub-Signature`.
Publisher content reserves `Content-Length`, `Content-Type`, `Host`, and
`X-Go-Publisher`. Result mapping reserves `Content-Length` and
`Transfer-Encoding` and reserves `Content-Type` when `Result.ContentType` is
set.

Validation MUST finish before mutating destination headers.

### 7.7 Detached mutable data

The framework MUST clone:

- inbound and outbound `http.Header` maps;
- `url.Values` maps and their value slices;
- request, result, update, and response body slices;
- messages before asynchronous queueing;
- callback metadata before every handoff.

Application mutation after a callback returns MUST NOT alter accepted work or
subsequent wire requests.

## 8. Handler request dispatch

### 8.1 Common dispatch

`Handler.ServeHTTP` executes this order:

1. If the method is not POST, return 405 and `Allow: POST`.
2. If admission is closed, return 503.
3. Require and parse `Content-Type`.
4. Reject more than one `X-Go-Publisher` field value.
5. Normalize the single publisher header value by trimming space and converting
   it to lowercase.
6. Select the publisher event, publisher content, or form path.
7. Bound and parse the body for the selected path.
8. Dispatch the operation.

Malformed input receives a bounded, generic response. Internal details and
application errors MUST NOT be reflected by the ordinary WebSub paths.

### 8.2 Form dispatch

Without publisher content selection, the request body is parsed as
`url.Values` and `hub.mode` selects:

| Mode | Condition |
|---|---|
| `subscribe` | always available |
| `unsubscribe` | always available |
| `register` | publisher extension enabled |
| `deregister` | publisher extension enabled |
| `publish` | invalid unless selected as event or content publication |

An unknown or invalid mode returns 400.

### 8.3 Subscription parsing

For `subscribe`:

1. Require one topic and callback.
2. Validate and normalize both URLs.
3. Parse and validate the optional lease.
4. Parse and validate the optional secret.
5. Remove reserved fields from detached extra parameters.
6. Construct `Subscription` using the configured public hub URL.

For `unsubscribe`, use the same algorithm except that
`hub.lease_seconds` is discarded without validation and is not retained.

## 9. Subscription and unsubscription execution

### 9.1 Admission algorithm

For a parsed subscription or unsubscription:

1. Snapshot request metadata.
2. Create a `Controller` whose external-verification permission comes from
   configuration.
3. Invoke the optional admission callback synchronously.
4. Map callback error or `Result`. If rejected or redirected, stop.
5. Clone the message, metadata, and result.
6. Construct an asynchronous job using `context.WithoutCancel` on the request
   context.
7. Attempt non-blocking admission to the bounded queue.
8. If the handler is closed or the queue is full, return 503 and do not accept
   the work.
9. Write exactly 202 using the safe result headers and body.
10. Release the queued job for worker execution only after the response is
    committed.

Step 10 is REQUIRED: validation and verification MUST NOT begin before the 202
response has been committed.

A `Result` status of zero or 202 accepts admission. A 4xx or 5xx status rejects
with that status and schedules no work. Other 2xx statuses and all 3xx statuses
inside `Result` are invalid and return 500.

A `RedirectError` MAY request only 307 or 308 with an absolute HTTP(S) location
without userinfo or fragment. A valid redirect schedules no work.

### 9.2 External verification controller

`Controller.MarkVerified` is available only during admission. When external
verification is disabled it returns `ErrExternalVerificationDisabled`. The first
enabled call succeeds; subsequent calls return `ErrAlreadyVerified`.

A marked job skips the outbound challenge but still enters the bounded queue,
runs asynchronous validation, computes verification timing metadata, and invokes
the verified callback.

Enabling this feature transfers challenge freshness, request binding, replay
prevention, and audit responsibility to the application.

### 9.3 Worker context

A worker waits for the admission response gate, then derives a timeout context
from the request context without its original cancellation. Therefore request
context values survive after `ServeHTTP` returns, while the original request
cancellation and deadline do not.

The worker context ends at the earliest of:

- `VerificationTimeout`;
- handler shutdown cancellation;
- completion of the job.

### 9.4 Asynchronous validation

The optional mode-specific validation callback runs before intent verification.
A nil callback allows processing to continue.

An unsubscription validation callback MAY enforce hub policy or inspect
application state, but MUST NOT perform publisher validation.

If validation returns `DeniedError`, the framework sends a best-effort GET to
the exact callback with:

- `hub.mode=denied`;
- `hub.topic`;
- optional sanitized `hub.reason`.

Carriage return, line feed, and NUL are removed from reasons, and the result is
bounded to 256 bytes.

Any other validation error is logged without the error value. When
`EnableHubErrorCallback` is true, the framework sends a best-effort
`hub.mode=hub-error` callback with reason `validation failed`.

A validation failure MUST NOT invoke a verified callback.

### 9.5 Challenge generation

Each non-externally-verified job creates a new challenge by:

1. reading 32 bytes from `crypto/rand`;
2. encoding them with unpadded URL-safe base64.

The result is 43 ASCII characters. Challenge generation failure terminates the
job and is logged without sensitive data.

### 9.6 Intent-verification request

The framework sends GET to the exact normalized callback. It appends encoded
parameters to the existing `RawQuery`:

| Mode | Parameters |
|---|---|
| subscribe | `hub.mode`, `hub.topic`, `hub.challenge`, `hub.lease_seconds` |
| unsubscribe | `hub.mode`, `hub.topic`, `hub.challenge` |

If `RawQuery` is non-empty, an ampersand is appended followed by the new encoded
parameters. Existing reserved escapes, field order, duplicate keys, empty
values, and overlapping `hub.*` keys are not parsed or overwritten.

Redirects are refused. The response body is bounded by `MaxCallbackBody`.
Verification succeeds only when the status is any 2xx and the complete body
equals the challenge byte-for-byte. Every 3xx, 4xx, 5xx, network error,
oversized body, or mismatch fails verification.

A verification failure MUST NOT invoke the verified callback.

### 9.7 Verified handoff

After subscription verification:

1. Select the requested lease or `DefaultLease`.
2. Clamp it to `MaxLease`.
3. Encode it as `EffectiveLeaseSeconds`.
4. Record `LeaseStartedAt` immediately before challenge processing, or at the
   equivalent point for externally verified work.
5. Invoke `OnSubscriptionVerified` with detached values.

After unsubscription verification, invoke `OnUnsubscriptionVerified` with a
detached `VerifiedUnsubscription`.

A verified callback error is logged without the error value. When
`EnableHubErrorCallback` is true, the framework sends a best-effort
`hub.mode=hub-error` callback with reason `verified callback failed`.

The framework MUST NOT persist, renew, expire, or delete application state.
Applications mutate state only from verified callbacks and MUST leave prior
state unchanged after failed renewal verification.

### 9.8 Status callbacks

Denied and hub-error status callbacks:

- use GET and the exact callback query-preservation algorithm;
- refuse redirects;
- bound and close the response body;
- do not require a particular response status or body;
- are best effort, and their errors do not re-enter application callbacks.

`hub-error` is a project-specific compatibility extension.

## 10. Result and error mapping

### 10.1 Result validation

A non-zero `Result.StatusCode` MUST be between 200 and 599. Headers and content
type are validated before writing.

When `Body` is nil or zero-length, the operation default body is used. A
non-empty body replaces the default body.

When a body exists and no content type is selected, the default content type is
`application/x-www-form-urlencoded`.

### 10.2 Operation defaults

| Operation | Default status | Default body |
|---|---:|---|
| subscribe admission | 202 | none |
| unsubscribe admission | 202 | none |
| register | 200 | `hub.mode=accepted` |
| deregister | 200 | `hub.mode=accepted` |
| event update | 202 | `hub.mode=accepted` |
| content update | 202 | `hub.mode=accepted` |

### 10.3 Admission errors

- malformed input returns 400;
- unsupported methods return 405 with `Allow: POST`;
- closed admission or exhausted verification capacity returns 503;
- `ErrDenied` from an admission callback returns 400;
- an unexpected admission callback error returns 500;
- an invalid callback `Result` returns 500.

Ordinary error responses use generic HTTP status text. Secrets and callback
causes are not exposed.

### 10.4 Callback panic containment

A panic from any application callback is recovered and converted to the generic
error `websubhub: application callback panicked`. The panic value and stack are
not exposed. It then follows the error mapping for that callback phase.

## 11. Single-subscriber content delivery

### 11.1 Construction

`NewDeliveryClient` snapshots one `Subscription` and transport configuration.
It validates:

- hub, topic, and callback as absolute HTTP(S) URLs;
- hub and callback without userinfo or fragments;
- secret length below 200 bytes;
- non-negative timeout and response limit.

The subscription snapshot defines the callback target, HMAC key, and current
Link metadata for every delivery by that client.

### 11.2 Delivery algorithm

`Deliver(ctx, content)` performs exactly one attempt:

1. Reject a nil client.
2. Validate the complete `ContentType`.
3. Validate custom headers and reserved fields.
4. Clone the body.
5. create POST to the exact callback URL;
6. copy safe custom headers;
7. set the complete `Content-Type`;
8. add separate `Link` values for the subscription hub (`rel=hub`) and topic
   (`rel=self`);
9. if the secret is non-empty, compute HMAC using the configured signature
   algorithm over the exact cloned body and set
   `X-Hub-Signature: <algorithm>=<lowercase-hex>`;
10. execute with redirect refusal and context propagation;
11. read, bound, snapshot, and close the response;
12. classify the result.

`HeaderHubSignature` is the exported name for `X-Hub-Signature`.
`SignatureAlgorithm` identifies the supported `sha256`, `sha384`, and `sha512`
values. The zero value selects `sha256`. The framework emits exactly one
signature and does not negotiate algorithms; the application MUST select an
algorithm supported by the subscriber.

The framework MUST NOT add a delivery identifier or other project-specific
delivery header. It MUST NOT retry.

### 11.3 Delivery result

Every 2xx response succeeds and returns a detached `DeliveryResponse`.

HTTP 410 returns `HTTPError` matching both `ErrDeliveryFailed` and
`ErrSubscriptionGone`. Other non-2xx responses, network errors, redirects, and
oversized bodies match `ErrDeliveryFailed`.

The bounded subscriber response body is exposed only for diagnostics and has no
WebSub protocol meaning.

### 11.4 Delivery ownership boundary

The application selects the body and content type and is responsible for their
completeness and correctness. It owns subscriber enumeration, concurrency,
ordering, retry budgets, acknowledgement, deduplication, dead-letter handling,
and the decision to stop after HTTP 410.

The current API derives `rel=self` and `rel=hub` from the subscription snapshot,
not from content-specific metadata. This behavior is normative for this edition
and prevents the package alone from claiming complete hub conformance.

## 12. Publisher extension and discovery

### 12.1 Isolation

The publisher extension is disabled by default. When disabled, register,
deregister, event, and content publication requests are rejected and extension
callbacks are not required.

Extension behavior MUST remain isolated from subscription and unsubscription
dispatch. `HeaderGoPublisher` is the exported name for `X-Go-Publisher`.

### 12.2 Inbound extension operations

| Operation | Wire form | Callback | Default response |
|---|---|---|---|
| register | form body: `hub.mode=register` and `hub.topic` | `OnRegisterTopic` | 200 accepted |
| deregister | form body: `hub.mode=deregister` and `hub.topic` | `OnDeregisterTopic` | 200 accepted |
| event | form body: `hub.mode=publish` and `hub.topic`; `X-Go-Publisher: event` | `OnUpdateMessage` with `UpdateEvent` | 202 accepted |
| content | query: `hub.mode=publish` and `hub.topic`; arbitrary content body | `OnUpdateMessage` with `UpdateContent` | 202 accepted |

Content publication accepts an absent publisher header or
`X-Go-Publisher: publish`. `PublisherClient.Publish` sends the explicit header.
A form-encoded `hub.mode=publish` without the event selector is rejected.

More than one `X-Go-Publisher` value is invalid. Header matching is
case-insensitive after trimming surrounding space.

A successful extension callback result is validated and written. A callback
error produces 400 with form fields `hub.mode=denied` and a sanitized
`hub.reason`. A `DeniedError` may supply the reason; other errors use
`operation denied`.

### 12.3 Publisher client

`PublisherClient` provides:

- `RegisterTopic`;
- `DeregisterTopic`;
- `Notify` for event-only publication;
- `Publish` for exact content.

Every request uses POST and propagates its context. Register, deregister, and
notify use form bodies. Publish preserves the exact body and complete content
type, appends `hub.mode` and `hub.topic` to any existing hub query, and sends
`X-Go-Publisher: publish`.

A publisher response succeeds only when its status is 2xx and its bounded form
body contains exactly one `hub.mode=accepted`. Every other response returns an
`HTTPError` matching `ErrPublisherFailed`. Redirects are refused.

### 12.4 Discovery Link helper

`AddDiscoveryLinks(header, self, hubs...)`:

1. requires a non-nil header map;
2. validates self as an absolute HTTP(S) topic URL;
3. requires at least one hub;
4. validates every hub as absolute HTTP(S) without userinfo or fragment;
5. completes all validation before mutation;
6. appends one `Link: <self>; rel="self"` value;
7. appends one `Link: <hub>; rel="hub"` value per hub;
8. preserves all existing header values.

The helper does not parse existing Link fields or prevent a pre-existing
`rel=self` from producing multiple self relations.

The extension is excluded from WebSub conformance because WebSub leaves the
publisher-to-hub notification mechanism unspecified.

## 13. Concurrency and lifecycle

### 13.1 Concurrency safety

`Handler`, `DeliveryClient`, and `PublisherClient` are safe for concurrent use
after construction. Application callbacks may run concurrently and are
responsible for their own shared state.

The framework MUST NOT create an unbounded goroutine, queue, response read, or
retry loop.

### 13.2 Admission accounting

Queue admission and handler closure are serialized. Each accepted job increments
the outstanding-work count exactly once. Queue rejection decrements it before
returning 503. Each processed or shutdown-drained job decrements it exactly once.

A worker MUST NOT execute a job before its 202 response gate opens.

### 13.3 Close algorithm

`Handler.Close(ctx)` is idempotent:

1. Mark admission closed exactly once.
2. Start waiting for all admitted jobs.
3. When all jobs finish, cancel worker context.
4. Wait for every worker to exit.
5. Signal completion to all callers.

If `ctx` ends first:

1. cancel remaining worker contexts;
2. drain queued jobs and balance their accounting;
3. return `ctx.Err()`.

A later `Close` call MAY wait for final completion and return nil. Requests
arriving after closure receive 503. The handler does not close caller-owned HTTP
clients, servers, brokers, stores, or application workers.

## 14. Security requirements and boundary

### 14.1 Transport and destination policy

The framework accepts HTTP and HTTPS because WebSub permits both. It does not
upgrade HTTP, configure server TLS, pin certificates, configure mutual TLS, or
detect downgrade.

Absolute URL validation is not an SSRF defense. Callback verification and
delivery can reach any address permitted by the injected transport, including
loopback, private, link-local, metadata-service, proxy-selected, or
DNS-rebound destinations. Production applications MUST enforce destination
policy at dial time using an appropriate `http.Transport` or network boundary.

The configured `HubURL` is authoritative. The framework MUST NOT derive it from
`Host`, `Forwarded`, or `X-Forwarded-*` headers.

### 14.2 Authentication and authorization

Inbound authentication and authorization belong in ordinary `net/http`
middleware. Topic registration, subscription admission, publisher access, and
tenant policy belong to application callbacks.

The framework copies inbound headers into `RequestMetadata` but does not
interpret `Authorization`. Applications MUST treat metadata as sensitive.

### 14.3 Secrets and privacy

The framework MUST NOT log:

- `hub.secret`;
- authorization values;
- callback capability queries;
- request or response payload bodies;
- application callback errors or panic values.

Topic and callback URLs may identify users or tenants. A callback query may be a
bearer capability and MUST be protected by application storage, logging, and
access policy.

HMAC covers only delivery body bytes. It does not authenticate Link,
`Content-Type`, or custom headers. Applications requiring header integrity or
confidentiality MUST use authenticated TLS or separate protection.

### 14.4 Resource safety

The framework provides finite body limits, verification timeout, worker count,
and queue capacity. These are not a complete denial-of-service defense.
Authentication, per-principal quotas, connection limits, distributed admission,
rate limiting, and abuse detection remain application responsibilities.

### 14.5 Redirects and client ownership

All framework-created outbound clients refuse redirects. A valid 307 or 308 from
an admission callback is an inbound response decision and does not weaken
outbound redirect refusal.

A caller-supplied `http.Client` value is copied and never mutated or closed.
Its transport may still implement application-specific proxy, DNS, TLS, and
destination behavior.

### 14.6 External verification

`AllowExternalVerification` is a trust-boundary switch and is disabled by
default. An application enabling it MUST provide intent authenticity equivalent
to the framework challenge flow and own freshness, binding, replay prevention,
and audit.

## 15. WebSub conformance boundary

### 15.1 Framework-provided hub mechanics

The framework implements:

- subscription and unsubscription form parsing;
- exact 202 admission before asynchronous validation and verification;
- application denial and supported 307/308 admission redirects;
- random single-use challenge generation;
- exact challenge echo verification;
- positive finite effective lease selection;
- unreserved URL-character normalization;
- callback query preservation;
- one-attempt HMAC-SHA256, HMAC-SHA384, or HMAC-SHA512 delivery;
- exact callback, body, and complete content type transmission;
- subscription-derived hub and self Link metadata.

### 15.2 Required application mechanics

A complete hub application must additionally provide:

- canonical topic policy;
- storage of verified subscriptions only;
- atomic renewal and unsubscription replacement;
- mandatory lease expiry and removal;
- selection of the complete current representation and matching content type;
- content-specific discovery metadata where it differs from subscription
  metadata;
- subscriber enumeration and fan-out;
- bounded retry and continued attempts for active subscriptions;
- authorization and operational controls.

These responsibilities are deliberately outside the core framework and do not
authorize the framework to claim standalone W3C hub conformance.

### 15.3 Subscriber scope

The module does not implement a subscriber discovery client, subscription
request client, pending-intent store, verification callback server, renewal
scheduler, delivery receiver, or signature validator. It MUST NOT claim
subscriber conformance.

### 15.4 Project extensions

Register, deregister, publish, event, external verification, and `hub-error`
behavior are project-specific extensions. They MUST remain distinguishable from
the standardized subscription, unsubscription, verification, and delivery
paths.

## Appendix A. Implementation conformance checklist

A change to the framework is conforming only when applicable items below remain
true.

### A.1 Construction and type safety

- required callback combinations are rejected at construction;
- zero configuration selects finite defaults;
- invalid negative or fractional lease configuration is rejected;
- supplied clients and mutable values are not aliased or mutated;
- the core module remains standard-library-only.

### A.2 Handler wire behavior

- unsupported methods return literal 405 and `Allow: POST`;
- malformed input returns bounded generic errors;
- subscription and unsubscription acceptance is literal 202;
- validation and verification begin only after acceptance;
- queue saturation and closure return 503 without accepted work;
- redirect results permit only literal 307 and 308;
- form fields, URL bytes, content types, and callback queries follow Sections 7
  through 9.

### A.3 Verification and lifecycle

- challenges come from `crypto/rand` and match exactly;
- redirects, non-2xx responses, oversized bodies, and mismatches fail;
- verified callbacks run only after successful or explicitly external
  verification;
- cancellation and shutdown account for accepted work exactly once;
- concurrency tests use channels, contexts, and bounded deadlines rather than
  timing assumptions.

### A.4 Delivery and publisher clients

- wire tests assert literal methods, URLs, queries, headers, media types, and
  body bytes;
- HMAC vectors cover the exact transmitted body;
- callback and publisher response bodies are bounded and closed;
- every outbound client refuses redirects;
- delivery remains one attempt and publisher acceptance requires both 2xx and
  `hub.mode=accepted`.

### A.5 Security and errors

- secrets, credentials, callback capability queries, and payloads do not appear
  in framework logs or framework-generated error strings; transport errors are
  treated as sensitive;
- unsafe, reserved, and response-splitting headers are rejected;
- URL syntax validation is not described as SSRF protection;
- enabled and disabled extension and external-verification configurations are
  tested;
- typed failures preserve `errors.Is` and `errors.As` behavior.

## Appendix B. References

- [W3C WebSub Recommendation](https://www.w3.org/TR/websub/)
- [RFC 2119: Key words for use in RFCs](https://www.rfc-editor.org/rfc/rfc2119)
- [RFC 8174: Ambiguity of Uppercase vs Lowercase](https://www.rfc-editor.org/rfc/rfc8174)
- [Go `net/http` package](https://pkg.go.dev/net/http)
- [Go memory model](https://go.dev/ref/mem)
