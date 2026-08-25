# lib-websubhub

[![Go Reference](https://pkg.go.dev/badge/github.com/ayeshLK/lib-websubhub.svg)](https://pkg.go.dev/github.com/ayeshLK/lib-websubhub)
[![Release](https://img.shields.io/github/v/release/ayeshLK/lib-websubhub)](https://github.com/ayeshLK/lib-websubhub/releases/latest)
[![CI](https://github.com/ayeshLK/lib-websubhub/actions/workflows/ci.yml/badge.svg)](https://github.com/ayeshLK/lib-websubhub/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ayeshLK/lib-websubhub/branch/main/graph/badge.svg)](https://codecov.io/gh/ayeshLK/lib-websubhub)
[![License](https://img.shields.io/github/license/ayeshLK/lib-websubhub)](LICENSE)

Build WebSub hubs in Go without giving up control of storage, delivery, or
deployment architecture.

`lib-websubhub` is a composable, standard-library-only protocol framework. It
adapts WebSub HTTP requests to typed Go callbacks, performs asynchronous intent
verification, and provides focused clients for subscriber delivery and the
optional publisher extension. It fits into ordinary `net/http` applications
and does not impose a database, broker, scheduler, or authentication system.

> [!IMPORTANT]
> The API is pre-release and under active validation. This package supplies hub
> protocol mechanics, not a complete production hub or a standalone W3C
> conformance claim.

## Why lib-websubhub?

- **Idiomatic HTTP composition:** mount `Handler` on any `http.ServeMux` and
  use normal middleware, TLS, and server lifecycle controls.
- **Typed application boundaries:** receive subscriptions, unsubscriptions,
  publisher updates, and request metadata through explicit callbacks.
- **Safe protocol defaults:** bounded bodies and verification work, redirect
  refusal, context propagation, immutable callback snapshots, and secret-safe
  errors.
- **Exact content delivery:** preserve payload bytes and complete media types,
  with optional WebSub HMAC signatures.
- **No infrastructure lock-in:** keep persistence, fan-out, retry,
  acknowledgement, lease expiry, clustering, and observability in application
  code.
- **No core dependencies:** the published module and its tests use only the Go
  standard library.

| The package handles | Your application handles |
|---|---|
| Request parsing and `hub.mode` dispatch | Topic and subscription persistence |
| Subscription intent verification | Authorization and callback/topic SSRF policy |
| Bounded verification queues and shutdown | Lease expiry and renewal storage |
| One delivery attempt to one subscriber | Fan-out, retry, acknowledgement, and DLQ policy |
| Discovery links and publisher client mechanics | Brokers, clustering, TLS, quotas, and observability |

## Install

The module requires Go 1.25 or newer. Install the current release explicitly so
your build remains reproducible:

```sh
go get github.com/ayeshLK/lib-websubhub@v0.5.0
```

The module is the root package and is imported as `websubhub`:

```go
import websubhub "github.com/ayeshLK/lib-websubhub"
```

Because `v0.5.0` is a pre-v1 release, review the changelog before upgrading to a
new minor version.

### Try the runnable hub

The in-memory example is the quickest way to exercise subscription
verification and content delivery locally:

```sh
git clone https://github.com/ayeshLK/lib-websubhub.git
cd lib-websubhub
git checkout v0.5.0
cd examples/in-memory-websubhub
go run .
```

The hub listens at `http://localhost:9090/hub`. Follow the example's
[protocol walk-through](examples/in-memory-websubhub/README.md#manual-protocol-walk-through)
to register a topic, subscribe a callback, publish content, and unsubscribe.

## Create a hub handler

The minimal composition below abbreviates application persistence but shows the
required verified-state callbacks:

```go
service := websubhub.Service{
    OnSubscriptionValidation: func(
        ctx context.Context,
        request websubhub.Subscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Enforce topic policy, callback destination policy, and quotas.
        return nil
    },
    OnSubscriptionVerified: func(
        ctx context.Context,
        subscription websubhub.VerifiedSubscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Atomically persist or renew the verified subscription.
        return nil
    },
    OnUnsubscriptionVerified: func(
        ctx context.Context,
        unsubscription websubhub.VerifiedUnsubscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Remove the verified subscription from application state.
        return nil
    },
}

hub, err := websubhub.NewHandler(websubhub.Config{
    HubURL: "https://hub.example.com/websub",
}, service)
if err != nil {
    return err
}

mux := http.NewServeMux()
mux.Handle("/websub", hub)
server := &http.Server{
    Addr:              ":8443",
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,
}
```

Here, persistence, server startup, and shutdown are application code. Wrap
`hub` with ordinary `net/http` authentication, authorization, logging, or
metrics middleware as needed. Call `hub.Close(ctx)` during shutdown to drain
accepted verification work.

Successful subscription and unsubscription admission returns `202 Accepted`;
validation and subscriber intent verification then run asynchronously. State
changes belong in `OnSubscriptionVerified` and `OnUnsubscriptionVerified`,
which are required so verified work is never silently discarded.

Topics, hubs, and callbacks must be absolute HTTP(S) URLs. For inbound topic and
callback values, percent-encoded URL characters in the unreserved set are
decoded before application callbacks run; reserved escapes remain unchanged.
Lease and secret fields remain strings matching their WebSub form
representation. Applications must persist subscriptions and expire them using
`LeaseStartedAt` and `EffectiveLeaseSeconds`.

## Deliver content

`DeliveryClient` represents one immutable subscription and performs exactly
one delivery attempt. The application decides ordering, retry, acknowledgement,
and dead-letter behavior.

```go
client, err := websubhub.NewDeliveryClient(
    subscription,
    websubhub.DeliveryConfig{HTTPClient: outboundClient},
)
if err != nil {
    return err
}

response, err := client.Deliver(ctx, websubhub.ContentDistribution{
    ContentType: "application/json; charset=utf-8",
    Body:        payload,
})
if errors.Is(err, websubhub.ErrSubscriptionGone) {
    // Stop delivery and remove or deactivate the subscription.
}
```

Delivery preserves exact body bytes and the full `Content-Type`. When the
subscription has a secret, the client adds `X-Hub-Signature`. Redirects are
refused, responses are bounded, and HTTP `410 Gone` matches
`ErrSubscriptionGone`.

## Publisher extension and discovery

Publisher-to-hub notification is not standardized by WebSub. The optional
extension is disabled by default. Enable `Config.EnablePublisherExtension` and
provide all three publisher callbacks to accept topic registration,
deregistration, content publication, and event-only notification.
`PublisherClient` invokes those operations using `X-Go-Publisher`.

Topic registration may declare an expected complete media type while preserving
existing calls that omit it:

```go
err := publisher.RegisterTopic(
    ctx,
    "https://publisher.example.com/topics/orders",
    websubhub.WithTopicContentType("application/json; charset=utf-8"),
)
```

The declaration reaches `OnRegisterTopic` as `TopicRegistration.ContentType`.
Applications own its persistence and any mismatch policy; an actual published
or fetched representation's valid media type remains authoritative.

Use `AddDiscoveryLinks` to append standards-facing `rel=self` and `rel=hub`
Link headers:

```go
err := websubhub.AddDiscoveryLinks(
    response.Header(),
    "https://publisher.example.com/topics/orders",
    "https://hub.example.com/websub",
)
```

## Runnable examples

- [In-memory hub](examples/in-memory-websubhub/) — the smallest complete
  application composition.
- [Kafka-backed hub](examples/kafka-websubhub/) — durable event-driven state,
  revisioned snapshots, multiple hub instances, and per-subscription delivery
  consumers. Kafka dependencies remain isolated in its independent module.

## Security and conformance boundary

Syntactic HTTP(S) validation is not SSRF protection. Production applications
must enforce destination policy at dial time, authenticate and authorize
inbound requests, configure TLS, protect persisted secrets, rate-limit clients,
and enforce lease expiry. Outbound clients refuse redirects and do not mutate
caller-supplied `http.Client` values.

Read the [security policy](SECURITY.md) before exposing a hub to untrusted
networks.

## Documentation

- [Framework implementation specification](docs/spec.md) — for maintainers
- [Contributor guide](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [Go API reference](https://pkg.go.dev/github.com/ayeshLK/lib-websubhub)

## Development

See the [contributor guide](CONTRIBUTING.md) for validation commands, coverage
gates, example-module checks, and the release process.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).

Copyright 2026 Ayesh Almeida.
