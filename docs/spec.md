# Go WebSubHub framework specification

Status: Draft 0.2
Target: Go 1.25 or newer
Dependencies: Go standard library only
Protocol baseline: [W3C WebSub Recommendation, 2 June 2026](https://www.w3.org/TR/websub/)

Normative terms such as MUST, SHOULD, and MAY are interpreted as described by
RFC 2119/RFC 8174.

## 1. Purpose and boundary

The `websubhub` package is a thin WebSub protocol layer over Go's `net/http`.
It translates HTTP requests into typed WebSub messages, invokes
application-supplied callbacks, performs protocol-defined callback operations,
and translates callback results into HTTP responses.

The framework deliberately does not implement a complete hub platform.

The framework owns:

- HTTP parsing and `hub.mode` dispatch;
- WebSub parameter and media-type validation;
- subscription and unsubscription intent-verification requests;
- denial and compatibility error callbacks;
- typed messages, results, and HTTP errors;
- authenticated HTTP content delivery to one subscriber;
- publisher discovery helpers and the optional publisher client;
- bounded protocol work, cancellation, and safe HTTP defaults.

The application owns:

- topic authorization, registration, storage, and deletion;
- subscription storage and state transitions;
- broker resource creation and deletion;
- content persistence, fan-out, retry, acknowledgment, and dead-letter policy;
- clustering, snapshots, state replication, and node ownership;
- authentication, authorization, observability, health endpoints, and deployment.

This thin boundary is the framework's primary architectural constraint.

## 2. Scope

Version 1 provides:

1. an `http.Handler` for hub requests;
2. typed callback functions for application-defined hub behavior;
3. asynchronous validation and intent-verification orchestration;
4. a single-subscriber `DeliveryClient`;
5. a `PublisherClient` for the optional publisher extension;
6. WebSub discovery Link-header helpers;
7. typed error and response mapping.

Subscriber discovery, a subscriber callback server, storage, message brokers,
durable queues, and an opinionated in-memory hub are outside the core package.
Examples MAY implement these facilities using application code.

## 3. Architectural model

```text
HTTP request
    -> websubhub.Handler
        -> parse and validate protocol
        -> invoke Service callback
        -> write protocol response
        -> validate and verify asynchronously when required
        -> invoke verified callback

Application delivery worker
    -> websubhub.DeliveryClient
        -> subscriber callback

Publisher
    -> websubhub.PublisherClient
        -> hub endpoint
```

`Handler` never reads or writes application state. It does not enumerate
subscriptions, create delivery workers, or decide retry and dead-letter policy.

## 4. Package shape

The initial module contains one public package:

```text
github.com/ayeshLK/lib-websubhub/
    handler.go         HTTP adapter and callback orchestration
    service.go         Service callbacks and result types
    subscription.go    subscription types and verification
    delivery.go        single-subscriber delivery client
    publisher.go       publisher extension client and messages
    discovery.go       Link advertisement helpers
    errors.go          typed and sentinel errors
    internal/...       bounded parsers and verification workers
```

Internal packages are not API commitments.

## 5. Public API

The signatures define the intended contract. The API remains subject to
change while the module is pre-release.

```go
package websubhub

type Config struct {
    HubURL               string
    DefaultLease         time.Duration
    MaxLease             time.Duration
    MaxRequestBody       int64
    MaxCallbackBody      int64
    VerificationTimeout time.Duration
    VerificationWorkers int
    VerificationQueue   int
    HTTPClient           *http.Client
    Logger               *slog.Logger

    // Allows Controller.MarkVerified. Disabled by default.
    AllowExternalVerification bool

    // Enables register, deregister, publish, and notify extension modes.
    EnablePublisherExtension bool

    // Sends hub.mode=hub-error after a verified callback fails.
    EnableHubErrorCallback bool
}

func NewHandler(Config, Service) (*Handler, error)

type Handler struct { /* unexported */ }
func (h *Handler) ServeHTTP(http.ResponseWriter, *http.Request)
func (h *Handler) Close(context.Context) error

type RequestMetadata struct {
    Header     http.Header
    RemoteAddr string
}

type Result struct {
    StatusCode  int
    Header      http.Header
    ContentType string
    Body        []byte
}

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

type RegisterTopicFunc func(
    context.Context, TopicRegistration, RequestMetadata,
) (Result, error)

type DeregisterTopicFunc func(
    context.Context, TopicDeregistration, RequestMetadata,
) (Result, error)

type UpdateMessageFunc func(
    context.Context, UpdateMessage, RequestMetadata,
) (Result, error)

type SubscriptionFunc func(
    context.Context, Subscription, RequestMetadata, *Controller,
) (Result, error)

type SubscriptionValidationFunc func(
    context.Context, Subscription, RequestMetadata,
) error

type SubscriptionVerifiedFunc func(
    context.Context, VerifiedSubscription, RequestMetadata,
) error

type UnsubscriptionFunc func(
    context.Context, Unsubscription, RequestMetadata, *Controller,
) (Result, error)

type UnsubscriptionValidationFunc func(
    context.Context, Unsubscription, RequestMetadata,
) error

type UnsubscriptionVerifiedFunc func(
    context.Context, VerifiedUnsubscription, RequestMetadata,
) error

type Controller struct { /* unexported */ }
func (c *Controller) MarkVerified() error
```

Function fields match Go's normal callback style and allow optional operations
without forcing applications to implement a large interface containing no-op
methods. All supplied callbacks MUST be safe for concurrent invocation.

`OnSubscriptionVerified` and `OnUnsubscriptionVerified` are required because
they are the application handoff after verified intent. When
`EnablePublisherExtension` is true, the three publisher callbacks are required.
The initial subscription, unsubscription, and validation callbacks are optional
and use documented defaults.

`NewHandler` validates callback combinations. No compiler plugin, code
generation, reflection-based method discovery, or custom static analyzer is
required.

## 6. Message types

```go
type Mode string

const (
    ModeSubscribe   Mode = "subscribe"
    ModeUnsubscribe Mode = "unsubscribe"
    ModeRegister    Mode = "register"
    ModeDeregister  Mode = "deregister"
    ModePublish     Mode = "publish"
)

type TopicRegistration struct {
    Mode  Mode
    Topic string
}

type TopicDeregistration struct {
    Mode  Mode
    Topic string
}

// Subscription represents a parsed WebSub subscription request while
// retaining additional form parameters explicitly.
type Subscription struct {
    Hub          string
    Mode         Mode
    Callback     string
    Topic        string
    LeaseSeconds string // subscriber-requested value; empty when omitted
    Secret       string
    Parameters   url.Values
}

// The distinct type proves that intent verification succeeded. The effective
// lease is kept separate from the subscriber's requested value.
type VerifiedSubscription struct {
    Subscription
    EffectiveLeaseSeconds string
    LeaseStartedAt        time.Time
}

type Unsubscription struct {
    Mode       Mode
    Callback   string
    Topic      string
    Secret     string
    Parameters url.Values
}

type VerifiedUnsubscription struct {
    Unsubscription
}

type UpdateKind uint8

const (
    UpdateEvent UpdateKind = iota
    UpdateContent
)

type UpdateMessage struct {
    Kind        UpdateKind
    Topic       string
    ContentType string
    Body        []byte
    Header      http.Header
}
```

Unknown subscription and unsubscription form parameters are preserved but
reserved WebSub parameters are exposed only through their typed fields. Mutable
values are cloned before callback or asynchronous use. Topic registration and
deregistration are closed protocol messages containing only mode and topic.

Protocol-facing lease values and secrets are strings, matching the form wire
format. Lease strings contain positive decimal
seconds. An empty lease string means the subscriber omitted the parameter. An
empty secret means no secret; an explicitly supplied empty secret is rejected.
Secret length is measured in bytes and MUST be less than 200 bytes. Secrets MUST
NOT appear in logs or error strings.

Application-specific metadata such as broker queue names, node identifiers, and
stale-state flags does not belong in these protocol messages. Applications can
associate it with `(topic, callback)` in their own state model.

## 7. HTTP request processing

The handler accepts `POST`. Other methods receive `405 Method Not Allowed` and
`Allow: POST`. It MUST:

1. limit the body before parsing;
2. require `application/x-www-form-urlencoded` for subscription, unsubscription,
   registration, deregistration, and event-only extension requests;
3. parse media types using `mime.ParseMediaType`;
4. require UTF-8 for form requests and reject explicitly incompatible form
   charsets;
5. validate arbitrary publisher and distribution content types without
   normalizing them or removing parameters;
6. reject missing, empty, or duplicated required fields;
7. reject malformed percent encoding, invalid URLs, invalid subscription leases,
   and oversized bodies;
8. accept and discard `hub.lease_seconds` on unsubscription without parsing or
   exposing it;
9. preserve unknown parameters and cloned headers;
10. return safe bounded error bodies.

Topic, hub, and callback values must be absolute HTTP(S) URLs. This enforces the
WebSub definition of a topic as an HTTP or HTTPS resource URL before application
callbacks run. Callback URLs with userinfo or fragments are rejected.

After form or query decoding, the handler decodes percent-encoded ASCII
unreserved characters (`ALPHA`, `DIGIT`, `-`, `.`, `_`, and `~`) in inbound
topic and callback URLs. Reserved escapes, non-ASCII escapes, ordering, and all
other URL bytes remain unchanged. Subscription, unsubscription, and publisher
extension callbacks receive these normalized values. Applications may impose
additional topic canonicalization, authorization, and deployment-specific
callback or SSRF policy in callbacks or outer middleware; such policy must also
be enforced by the application's outbound transport.

## 8. Callback result mapping

A zero `Result` selects the operation default:

| Callback | Default success |
|---|---|
| register/deregister | 200 and `hub.mode=accepted` |
| update | 202 |
| subscription/unsubscription | exactly 202 |

For subscription and unsubscription admission, status zero or 202 means
accepted and the handler emits exactly `202 Accepted`. Any other 2xx or any 3xx
returned through `Result` is an invalid callback result and produces 500. A
4xx or 5xx `Result` is an explicit admission rejection and no verification is
scheduled. Redirects use `RedirectError`, not `Result`.

`Result` otherwise permits safe headers, content type, and body customization.
The framework validates status codes and rejects hop-by-hop or
response-splitting headers. Arbitrary valid content types retain their complete
field value, including parameters.

Errors support `errors.Is` and `errors.As`:

```go
var (
    ErrInvalidRequest     = errors.New("websubhub: invalid request")
    ErrDenied             = errors.New("websubhub: denied")
    ErrVerificationFailed = errors.New("websubhub: verification failed")
    ErrSubscriptionGone   = errors.New("websubhub: subscription gone")
    ErrDeliveryFailed     = errors.New("websubhub: delivery failed")
    ErrQueueFull          = errors.New("websubhub: verification queue full")
    ErrClosed             = errors.New("websubhub: closed")
)

type HTTPError struct {
    Operation  string
    StatusCode int
    Header     http.Header
    Body       []byte
    Err        error
}

type DeniedError struct {
    Reason string
    Err    error
}

type RedirectError struct {
    StatusCode int
    Location   string
}
```

`DeniedError` from an asynchronous validation callback causes a denied
notification. `RedirectError` is valid from the initial subscription or
unsubscription callback and only for 307 or 308. Its Location must be an
absolute HTTP(S) URL without userinfo or a fragment.

## 9. Subscription lifecycle

### 9.1 Initial callback

After protocol parsing, `OnSubscription` or `OnUnsubscription` runs
synchronously. A nil function means accept. It may reject, redirect, customize
safe acceptance headers or body, or—when explicitly enabled—mark intent as
already verified through `Controller`. Successful admission always produces
exactly `202 Accepted`.

If asynchronous work cannot be admitted to the bounded verification queue, the
handler returns `503` and does not claim acceptance. Otherwise it sends `202`
before asynchronous validation or verification begins.

Initial callbacks receive the request context. Work that continues after `202`
uses a handler-owned context because `http.Request.Context` is cancelled when
`ServeHTTP` returns. The asynchronous context is cancelled by the verification
timeout or `Handler.Close`; request metadata needed later is copied explicitly.

### 9.2 Asynchronous validation

`OnSubscriptionValidation` runs after `202` and before subscriber verification.
It can check registered topics, authorization state, or eventually consistent
application state. A nil callback allows the request.

If it returns `DeniedError`, the framework sends a callback `GET` containing:

- `hub.mode=denied`;
- `hub.topic`;
- optional safe `hub.reason`.

Unexpected validation failures are logged and reported through an optional
hub-error callback when enabled.

`OnUnsubscriptionValidation` follows the same application hook model, but an
application must not use it for publisher validation forbidden by WebSub.

### 9.3 Intent verification

Unless `Controller.MarkVerified` was used, the framework sends a callback `GET`
with mode, topic, a cryptographically random single-use challenge, and the
effective lease for subscriptions. After the required unreserved-character
normalization during admission, the callback's existing `RawQuery` is preserved
byte-for-byte and the newly encoded parameters are appended after it. Existing
ordering, reserved escaping, duplicate keys, empty values, and overlapping
WebSub-named keys are not parsed, normalized, or overwritten.

Redirects are not followed. The response body is bounded. Verification succeeds
only for a 2xx status and a body exactly equal to the challenge.

`MarkVerified` is permitted only when `AllowExternalVerification` is true. By
calling it, the application asserts that it has performed equivalent intent
verification. The framework does not treat it as an automatic or unsafe bypass.

### 9.4 Verified callback

After successful verification, the framework invokes
`OnSubscriptionVerified` or `OnUnsubscriptionVerified`. This is the only state
handoff:

- an in-memory application can update a map;
- a database-backed application can commit a row;
- a broker-backed application can provision/delete a consumer and emit a state
  event;
- a clustered application can assign node ownership and replicate the change.

The framework performs none of these actions. Callback errors are logged. When
`EnableHubErrorCallback` is true, it sends the project-specific
`hub.mode=hub-error` notification with a safe reason.

For subscriptions, `EffectiveLeaseSeconds` is the hub-selected positive
decimal lease sent during verification and `LeaseStartedAt` is the time that
verification request was initiated. The framework does not persist
subscriptions, calculate a stored expiration timestamp, schedule expiry, or
delete expired state. Applications calculate and enforce expiry from these
values.

Applications must ensure that renewal replaces active state only after
verification and that failed verification leaves the previous state unchanged.

## 10. Single-subscriber content delivery

The package provides transport, not fan-out:

```go
type DeliveryConfig struct {
    HTTPClient     *http.Client
    Timeout        time.Duration
    MaxResponseBody int64
}

type ContentDistribution struct {
    ContentType string
    Body        []byte
    Header      http.Header
}

type DeliveryResponse struct {
    StatusCode int
    Header     http.Header
    Body       []byte
}

func NewDeliveryClient(Subscription, DeliveryConfig) (*DeliveryClient, error)
func (c *DeliveryClient) Deliver(
    context.Context, ContentDistribution,
) (DeliveryResponse, error)
```

Each delivery:

1. is bound to the hub, topic, callback, and secret of the `Subscription`
   supplied to `NewDeliveryClient`;
2. POSTs the exact body to the exact callback;
3. preserves callback query values;
4. validates and sends the complete supplied content type without removing
   charset, boundary, or other parameters;
5. adds `rel=hub` and `rel=self` Link values from the subscription;
6. adds `X-Hub-Signature: sha256=<hex>` when a secret exists;
7. refuses redirects;
8. returns success for any 2xx;
9. returns an error matching `ErrSubscriptionGone` for 410;
10. returns a typed `HTTPError` for other failures;
11. bounds and closes the response body.

The client does not add a delivery identifier or other project-specific
extension header.

The client performs one logical delivery attempt. The application decides
whether and when to retry, acknowledge, negatively acknowledge, dead-letter,
mark stale, or delete subscription state.

The client never mutates a caller-supplied `http.Client`. It copies the client
value and installs redirect refusal on the copy.

## 11. Publisher support

`AddDiscoveryLinks` appends one `rel=self` and one or more `rel=hub` Link
relations without overwriting unrelated fields.

WebSub leaves publisher-to-hub notification unspecified. The package implements
an optional project-specific extension:

- register topic;
- deregister topic;
- publish content;
- notify that a topic changed;
- parse `X-Go-Publisher: publish|event` to select the update mode.

```go
type PublisherClientConfig struct {
    HubURL          string
    HTTPClient      *http.Client
    MaxResponseBody int64
}

func NewPublisherClient(PublisherClientConfig) (*PublisherClient, error)
func (c *PublisherClient) RegisterTopic(context.Context, string) error
func (c *PublisherClient) DeregisterTopic(context.Context, string) error
func (c *PublisherClient) Publish(context.Context, UpdateMessage) error
func (c *PublisherClient) Notify(context.Context, string) error
```

The extension is separately documented and excluded from the W3C conformance
claim.

## 12. Configuration defaults

Zero values select finite defaults; negative durations, counts, or byte limits
are invalid. DefaultLease and MaxLease must resolve to positive whole-second
durations because WebSub lease values are decimal seconds.

| Field | Default |
|---|---|
| default/max lease | 10 days / 10 days |
| request body | 64 KiB |
| callback response body | 4 KiB |
| verification timeout | 10 seconds |
| verification workers/queue | 4 / 1,024 |
| HTTP client | package-owned client with cloned `http.DefaultTransport` |
| logger | discard logger |
| external verification | disabled |
| publisher extension | disabled |
| hub-error compatibility callback | disabled |

Delivery client defaults are 30 seconds and 64 KiB.

## 13. Lifecycle and concurrency

- `ServeHTTP` and all clients are safe for concurrent use.
- Verification work uses a fixed worker count and bounded queue.
- `Handler.Close` is idempotent, stops admission, and drains accepted protocol
  work until its context ends.
- The handler does not close caller-owned HTTP clients or application resources.
- Applications close brokers, stores, workers, and servers themselves.
- Panics from callbacks are recovered at the framework boundary and converted to
  safe failures.
- Callback values are cloned before crossing goroutine boundaries.

The framework does not guarantee ordering between separate requests. The
application owns concurrency control for its topic and subscription state.

## 14. Security model and current limitations

This section maps the
[WebSub Security Considerations](https://www.w3.org/TR/websub/#security-considerations)
to the current package. "Application" below includes outer `net/http`
middleware, the hub implementation, subscriber software, and deployment
configuration. A feature marked unsupported is not made safe merely by using
the framework's default configuration.

| W3C concern | Current status | Responsibility and limitation |
|---|---|---|
| HTTPS for all requests | Not enforced | HTTP and HTTPS URLs are accepted. The package neither upgrades HTTP nor selects an HTTPS alternative. Applications must configure server TLS and require HTTPS by policy. |
| Safe discovery | Outside scope | `AddDiscoveryLinks` writes Link headers, but the package does not discover hubs from HTTP bodies or HTML. Subscriber discovery code must decide whether to inspect only the HTML `head`; untrusted `body` links must not silently select a hub. |
| HTTPS, unique capability callback URLs | Not supported | The package validates callback syntax but accepts HTTP callbacks and does not generate subscriber callback URLs. Subscribers must create unguessable URLs, serve them over HTTPS, and protect their lifecycle. |
| Subscriber use of `hub.secret` | Not enforced | A non-empty secret is accepted and preserved, but remains optional. The package does not generate secrets or assess their entropy. |
| Short-lived leases | Partially implemented | The default and maximum lease are 10 days, and requested leases are clamped to the configured maximum. Operators can configure a different maximum. Persistence and expiry of verified state remain application responsibilities. |
| Random, single-use challenge | Implemented | Each verification uses 32 bytes from `crypto/rand`, encoded as 43 URL-safe ASCII characters. Success requires a 2xx response whose complete bounded body exactly equals that challenge. |
| XSS-safe verification response | Subscriber-side; unsupported | The package does not provide a subscriber callback server and does not require a safe verification response media type or `X-Content-Type-Options: nosniff`. Subscribers must use a safe type such as `application/octet-stream` and set `nosniff`, as required by WebSub. The hub does not inspect these response headers. |
| Challenge download restrictions | Subscriber-side; unsupported | Subscriber implementations must bound and validate `hub.challenge`. Framework-generated challenges are fixed-length URL-safe text, but the package provides no subscriber request parser that enforces this. |
| Exact callback for distribution | Implemented | `DeliveryClient` posts to the callback captured in the subscription, including its scheme and query, and refuses redirects. |
| Signing when a secret exists | Implemented | `DeliveryClient` signs the exact transmitted body with HMAC-SHA256 and sends `X-Hub-Signature: sha256=<hex>`. |
| Signature algorithm choice | SHA-256 only | SHA-1, SHA-384, SHA-512, multiple signatures, and algorithm negotiation are not implemented. SHA-256 satisfies the W3C security recommendation to use at least SHA-256 when transport may be compromised. |
| Subscriber signature validation | Subscriber-side; unsupported | The package sends signatures but has no subscriber component to parse, verify in constant time, or reject invalid distribution requests. |
| Header integrity | Not provided by HMAC | WebSub HMAC covers only the request body. Link, content type, and application-supplied headers are not authenticated by the signature and must not be trusted without authenticated TLS or separate application protection. |
| Topic and callback URL privacy | Application-owned | URLs may contain identifying information and are exposed to application callbacks and protocol peers. The package provides no retention, erasure, access-control, or privacy-policy mechanism. |

### 14.1 Transport security

The package has no TLS configuration abstraction. Inbound TLS belongs to
`http.Server`; outbound trust roots, client certificates, proxies, DNS policy,
and TLS version policy belong to an injected `http.Client` and its
`http.Transport`. The package preserves an `https` callback and refuses
verification and delivery redirects, but its default clients also permit plain
HTTP. It does not implement automatic HTTP-to-HTTPS upgrade, certificate
pinning, mutual TLS configuration, or downgrade detection.

The configured public `HubURL` is authoritative. The handler does not derive it
from untrusted `Host`, `Forwarded`, or `X-Forwarded-*` headers.

### 14.2 Discovery

The core package is not a subscriber and does not parse HTML, Atom, RSS, or HTTP
response bodies for discovery. Consequently, it does not implement the W3C
recommendation to avoid attacker-supplied `link` elements in an HTML `body`.
Subscriber discovery code must define a safe parsing boundary, prefer HTTPS hub
links, and treat every discovered hub URL as untrusted input.

`AddDiscoveryLinks` only validates and appends application-supplied `rel=self`
and `rel=hub` Link values. It does not establish that the caller controls either
URL.

### 14.3 Subscription and intent verification

The framework implements hub-side challenge generation and comparison. A new
challenge is created for every subscribe or unsubscribe verification request;
it is never accepted through a later framework endpoint. Callback response
bodies and verification time are bounded, and redirects are refused.

The following controls are not currently provided:

- mandatory HTTPS for hub, topic, or callback URLs;
- subscriber capability-URL generation or callback access control;
- mandatory use, generation, strength checks, storage, or rotation of
  `hub.secret`;
- subscriber-side verification responses using a safe media type such as
  `application/octet-stream` and `X-Content-Type-Options: nosniff`;
- subscriber-side validation of challenge length and character set; and
- automatic expiration or deletion of application-owned subscription state.

When `AllowExternalVerification` is enabled, `Controller.MarkVerified` lets the
application assert that equivalent intent verification already occurred. The
framework cannot validate that assertion; enabling it transfers challenge
freshness, binding, replay prevention, and audit responsibility to the
application.

### 14.4 Authenticated distribution

`DeliveryClient` binds one delivery client to an immutable snapshot of the hub,
topic, callback, and secret. When a secret is present it always emits one
HMAC-SHA256 signature over the exact body. It does not sign headers. Applications
must therefore use HTTPS when header integrity or confidentiality matters.

The framework does not implement subscriber-side signature validation, replay
detection, delivery nonce validation, deduplication, retry limits, or
dead-letter policy. It also does not require a secret. Subscriber implementations
must parse the declared algorithm, compare signatures without timing leaks, and
discard a payload whose signature fails. Delivery retry and continued attempts
for active subscriptions remain hub-application responsibilities.

### 14.5 SSRF, abuse resistance, and content safety

Syntactic HTTP(S) URL validation is not an SSRF defense. By default, callback
verification and content delivery can connect to loopback, link-local, private,
metadata-service, and other addresses reachable by the process. DNS rebinding,
proxy or custom `RoundTripper` behavior, and address changes between validation
and dialing must be considered. Untrusted
deployments must apply callback/topic authorization and enforce destination
policy at dial time with a restricted `http.Transport` or network boundary.

The framework bounds request and response bodies, verification concurrency, and
the verification queue. These are resource bounds, not a complete denial-of-
service defense. It does not provide authentication, authorization, per-principal
quotas, rate limiting, connection limits, replay protection, audit logging,
abuse detection, or distributed admission control. The project-specific
publisher extension is disabled by default and must be protected with middleware
when enabled.

Publisher and distribution bodies are opaque bytes. The package validates media
type syntax but does not scan, sanitize, or classify HTML, scripts, malware, or
other active content. Subscribers must treat delivered content according to its
media type and rendering context.

### 14.6 Secrets, logging, and privacy

The core implementation does not log `hub.secret`, authorization values,
signatures, callback bodies, or distribution payloads. Applications receive
cloned request headers and protocol values, so their callbacks, middleware,
stores, and loggers must apply redaction and access control. Topic and callback
URLs may contain personally identifying or tenant-specific information. A
callback URL may intentionally be an unguessable bearer capability, so the
complete URL, including its query, must be treated as sensitive and excluded
from routine logs. Do not add unrelated credentials to protocol URLs.

The package has no durable storage and therefore provides no encryption at rest,
retention, erasure, tenant isolation, or backup policy. Those controls belong to
the application that persists topics and verified subscriptions. JWT, JWKS,
OAuth, Prometheus, OpenTelemetry, and vendor-specific security or observability
remain middleware/application concerns.

## 15. W3C conformance and ownership analysis

This section audits the package against the hub- and subscriber-facing flows in
the [W3C WebSub Recommendation](https://www.w3.org/TR/websub/). "Not included"
means the core package supplies no orchestration or state for that behavior; an
application may implement it around the package. The package alone is neither a
complete deployable hub nor a WebSub subscriber. Conformance is a property of the
assembled application.

The tables document current ownership boundaries.

The W3C defines separate
[hub](https://www.w3.org/TR/websub/#hubs) and
[subscriber](https://www.w3.org/TR/websub/#subscribers) conformance classes;
the boundaries below are grouped by the actor that must supply the behavior.

### 15.1 Discovery and canonical topic selection

The W3C [Discovery](https://www.w3.org/TR/websub/#discovery) flow requires a
subscriber to retrieve the topic with GET or HEAD, inspect HTTP Link headers
first, then inspect embedded HTML or XML links when headers are absent. It also
defines [content-negotiation](https://www.w3.org/TR/websub/#content-negotiation)
behavior through representation-specific `rel=self` URLs.

| Flow | Current support | Boundary or owner |
|---|---|---|
| Publisher advertises `rel=self` and one or more `rel=hub` links | Partial | `AddDiscoveryLinks` appends HTTP Link headers. It does not generate embedded HTML, Atom, or RSS links. |
| Subscriber GET/HEAD discovery and Link parsing order | Not included | There is no discovery client or Link-header parser. |
| Embedded HTML/XML discovery | Not included | There is no HTML, Atom, RSS, or generic XML parser. |
| Representation and language negotiation | Not included | Publisher and subscriber code must agree on and retain the discovered canonical self URL. |
| Subscribe to one or more advertised hubs | Not included | There is no subscriber-side multi-hub selection, failover, or duplicate-event policy. |
| Confirm that `hub.topic` is the publisher-advertised self URL | Application policy | The framework validates an absolute HTTP(S) URL but does not fetch the topic or compare discovery metadata. `OnSubscriptionValidation` may enforce this. |

### 15.2 Subscriber request client and subscription state

The package does not implement the subscriber actor described by
[Subscribing and Unsubscribing](https://www.w3.org/TR/websub/#subscribing-and-unsubscribing)
and [Subscriber Sends Subscription Request](https://www.w3.org/TR/websub/#subscriber-sends-subscription-request).
There is no `SubscriberClient` or subscriber state machine.

The following subscriber responsibilities are not included:

- constructing subscribe and unsubscribe form requests and attaching
  hub-specific headers or credentials;
- generating a unique capability callback URL and cryptographically random
  `hub.secret`;
- following a hub's 307 or 308 response and retrying at the new hub URL;
- recording pending intent keyed by topic, hub, callback, and mode before sending
  the request;
- handling the asynchronous delay between HTTP 202 and verification;
- persisting the verified effective lease and scheduling renewal before expiry;
- changing callback capability URLs or secrets on renewal;
- processing a later `hub.mode=denied` notification; and
- coordinating subscriptions to multiple hubs.

### 15.3 Hub subscription lifecycle

The handler implements the core HTTP and verification parts of
[subscription requests](https://www.w3.org/TR/websub/#subscriber-sends-subscription-request),
[subscription validation](https://www.w3.org/TR/websub/#subscription-validation),
and [intent verification](https://www.w3.org/TR/websub/#hub-verifies-intent):
form parsing, exact 202 acceptance, optional asynchronous validation, denial
callbacks, 307/308 initial redirects, random challenges, exact challenge-body
comparison, percent-encoded unreserved-character normalization, and verified
callbacks. It also permits renewal requests to proceed to verification.

The following hub lifecycle behavior is not owned or enforced by the framework:

| Requirement or flow | W3C level | Current boundary |
|---|---|---|
| Optional publisher validation | MAY | `OnSubscriptionValidation` can call publisher-specific code, but the package defines no standard publisher-validation protocol or client. |
| No publisher validation during unsubscription | Required flow rule | The framework makes no publisher call itself. It cannot prevent an application from incorrectly doing so inside `OnUnsubscriptionValidation`; that callback must be limited to hub policy and state validation. |
| Atomic renewal and unsubscription override | MUST | The framework emits verified messages but has no state store or transaction boundary. The application must replace or remove the topic/callback entry only after verification and leave prior state unchanged on failure. |
| Mandatory lease expiration | MUST | The framework calculates `EffectiveLeaseSeconds` and `LeaseStartedAt` but does not expire or delete state. A conforming hub application must enforce expiry and must not create perpetual subscriptions. |
| Optional periodic reconfirmation | OPTIONAL | No scheduler or reconfirmation API is provided. |
| Durable recovery | Not specified | Pending verification jobs and verified subscription state are in memory or application-owned; no restart recovery, reconciliation, or clustered ownership is provided. |

Synchronous admission callbacks must not be used to perform the asynchronous
publisher validation described by WebSub. If they perform slow external work,
the initial response time and the intended 202-before-validation flow become
application-dependent.

### 15.4 Subscriber intent callback

The subscriber side of
[Verification Details](https://www.w3.org/TR/websub/#verification-details) is
not included. The package has no callback handler that:

- distinguishes intent GET requests from distribution POST requests;
- matches `hub.mode` and `hub.topic` against locally pending intent;
- validates the challenge character set and length;
- returns the exact challenge with 2xx for approved intent or 404 otherwise;
- emits a safe response media type and `X-Content-Type-Options: nosniff`;
- ignores `hub.lease_seconds` on unsubscription;
- commits local subscriber state only after accepting verification; or
- receives and acts on later `hub.mode=denied` notifications.

Subscriber-facing APIs are outside the current module scope.

### 15.5 Publishing and subscription migration

WebSub deliberately leaves the publisher-to-hub notification mechanism open in
[Publishing](https://www.w3.org/TR/websub/#publishing). The package's
`PublisherClient` and register, deregister, event, and content modes are
project-specific extensions, not W3C-defined wire operations.

[Subscription Migration](https://www.w3.org/TR/websub/#subscription-migration)
is subscriber- and publisher-driven and is not implemented. There is no
subscriber renewal process that re-fetches the old topic, follows its redirect,
discovers a new self URL or hub, and establishes replacement state. The previous
hub requires no special migration operation, so none is added to the hub API.

### 15.6 Hub content distribution

`DeliveryClient` implements one POST attempt, preserves the exact callback and
content type, sends the supplied body, classifies response status, refuses
redirects, adds Link headers, and signs with HMAC-SHA256 when a secret exists.
Signing follows
[Authenticated Content Distribution](https://www.w3.org/TR/websub/#signing-content).
The remaining
[Content Distribution](https://www.w3.org/TR/websub/#content-distribution)
workflow is application-owned or absent:

| Requirement or flow | W3C level | Current boundary |
|---|---|---|
| Select and distribute the full current representation | MUST | The framework cannot fetch, persist, or determine completeness of topic content. `ContentDistribution.Body` is trusted as complete. |
| Match the topic representation's Content-Type | MUST | The client preserves the complete caller-supplied value but cannot fetch the topic or verify that the value matches its current representation. |
| Atom/RSS reduced-feed delivery | MAY | No format-aware diff or already-delivered entry filtering is provided. |
| Ignore subscriber response bodies | MUST | The client reads a bounded response body and exposes it in `DeliveryResponse` for diagnostics. Applications must not assign WebSub protocol meaning to it. |
| Fan-out and ordering | Required hub behavior; ordering unspecified | The client targets one subscription. Enumeration, concurrency, ordering, backpressure, and per-topic sequencing are application responsibilities. |
| Retry with limits | SHOULD | No automatic retry, delay, backoff, jitter, or retry budget is provided. |
| Keep a failing subscription active | MUST | The client reports failure but has no subscription state. The application must retain it until lease expiry and attempt later updates despite earlier failures. |
| HTTP 410 handling | MAY | `ErrSubscriptionGone` is returned, but termination is an application decision. |
| Delivery deduplication | Not specified | No event identifier semantics, replay ledger, idempotency key, or deduplication store is defined. |

### 15.7 Subscriber content distribution

The package does not provide the subscriber callback POST flow or the
[signature-validation](https://www.w3.org/TR/websub/#signature-validation)
flow. A subscriber implementation still needs to:

- accept arbitrary topic media types and retain the exact request bytes needed
  for signature verification;
- locate local active subscription state from its callback capability, not from
  distribution Link headers;
- require `X-Hub-Signature` when its subscription used a secret;
- parse a recognized algorithm and verify HMAC using a timing-safe comparison;
- discard missing or invalid authenticated deliveries;
- treat headers as unauthenticated unless protected by HTTPS;
- return a prompt 2xx to acknowledge receipt or optionally 410 for a deleted
  subscription; and
- separate receipt acknowledgment from asynchronous content processing.

No subscriber persistence, handler middleware, message dispatch, deduplication,
or callback lifecycle is supplied.

### 15.8 Conformance consequence

A complete hub built on this package must add at least verified subscription
storage, atomic renewal and removal, mandatory lease expiry, complete-content
selection, content-correct discovery metadata, fan-out, bounded retry, and
continued delivery attempts for active subscriptions. Optional publisher
validation and periodic reconfirmation may be omitted because WebSub marks them
optional.

A complete subscriber requires a separate discovery client, subscription client,
pending-intent store, safe verification/denial handler, renewal and migration
scheduler, distribution callback, and authenticated-delivery validator. The
current module should not claim subscriber conformance.

## 16. Standard-library and extension policy

The core module has no third-party dependencies. Broker-backed applications may
use external Kafka, JMS, Solace, database, authentication, or telemetry modules
without adding those dependencies to `websubhub` itself.

No compiler plugin is planned. Go's compiler validates callback function types,
`NewHandler` validates required callback presence and configuration, and normal
tests validate runtime combinations. A custom `go vet` analyzer remains outside
scope while compilation and constructor validation express all API invariants.

## 17. Conformance boundary

The framework implements the protocol portions below. Because the module
intentionally delegates MUST-level behavior described in Section 15, this list
defines an implementation boundary rather than a standalone W3C hub conformance
claim. A complete assembled hub may claim conformance only after supplying the
missing behavior.

- subscription and unsubscription request parsing;
- exact 202 acceptance, denial, redirects, and intent verification;
- finite effective lease selection and verification start metadata;
- decoding of percent-encoded unreserved topic and callback URL characters;
- preservation of the normalized callback query with appended protocol parameters;
- authenticated single-subscriber delivery;
- complete delivery content type and subscription-bound Link headers.

Application conformance responsibilities include:

- canonical topic validation;
- storing only verified subscriptions;
- atomic renewal and unsubscription semantics;
- lease expiration in application state;
- complete-content selection;
- delivery retry policy and continued attempts for active subscriptions;
- topic, subscriber, and publisher authorization.

The publisher and hub-error behaviors are project-specific extensions and are
excluded from the standards claim.

## 18. Compatibility policy

The module follows semantic versioning. Exported API removals or incompatible
semantic changes require a major version. Protocol fixes may tighten rejection
of invalid input in a minor release and must be called out in release notes.

The initial minimum is Go 1.25. New framework releases support the two Go major
versions supported by the Go project at release time.
