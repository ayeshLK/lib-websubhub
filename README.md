# lib-websubhub

A thin, standard-library-only WebSub protocol framework for Go. It adapts
WebSub HTTP requests to application callbacks; your application owns topic and
subscription storage, authorization, fan-out, retry, and broker semantics.

The module currently targets Go 1.25 or newer. The source uses only Go standard
library packages.

The public API is under active validation and should be treated as pre-release.

Install a released version with:

```sh
go get github.com/ayeshLK/lib-websubhub
```

The package name is `websubhub`, so normal imports read naturally:

```go
import "github.com/ayeshLK/lib-websubhub"
```

## Repository layout

- The repository root is the published Go module.
- [`examples/in-memory-websubhub/`](examples/in-memory-websubhub/) is a runnable,
  application-owned in-memory hub and an independent Go module.
- [`docs/spec.md`](docs/spec.md) defines the framework contract, protocol
  behavior, and explicit conformance boundaries.
- [`docs/testing-plan.md`](docs/testing-plan.md) defines the test matrix and
  coverage policy.
- [`docs/release-plan.md`](docs/release-plan.md) defines Go compatibility, CI,
  and publication policy.

## Hub handler

```go
service := websubhub.Service{
    OnSubscriptionValidation: func(
        ctx context.Context,
        request websubhub.Subscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Apply topic policy, callback/SSRF policy, quotas, and authorization.
        return nil
    },
    OnSubscriptionVerified: func(
        ctx context.Context,
        subscription websubhub.VerifiedSubscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Persist or replace application-owned subscription state.
        return nil
    },
    OnUnsubscriptionVerified: func(
        ctx context.Context,
        unsubscription websubhub.VerifiedUnsubscription,
        metadata websubhub.RequestMetadata,
    ) error {
        // Remove application-owned subscription state.
        return nil
    },
}

hub, err := websubhub.NewHandler(websubhub.Config{
    HubURL: "https://hub.example.com/websub",
}, service)
if err != nil {
    log.Fatal(err)
}
defer hub.Close(context.Background())

// Routing, authentication, TLS, timeouts, and server shutdown remain ordinary
// net/http concerns. Wrap hub with your middleware before mounting it.
mux := http.NewServeMux()
mux.Handle("/websub", authenticate(hub))
server := &http.Server{
    Addr:              ":8443",
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,
}
log.Fatal(server.ListenAndServeTLS("server.crt", "server.key"))
```

Both verified callbacks are required because successful verification must never
silently discard state changes. Synchronous admission and asynchronous
validation callbacks are optional. Successful subscription and unsubscription
admission always returns exactly `202 Accepted`.

Protocol-facing `LeaseSeconds`, `EffectiveLeaseSeconds`, and `Secret` values
are strings, matching their WebSub form-field representation. Applications
persist subscriptions and calculate expiration from `LeaseStartedAt` plus
`EffectiveLeaseSeconds`; the framework does not store or expire subscriptions.

Topics must be absolute HTTP(S) resource URLs, as required by WebSub.

## Content delivery

```go
client, err := websubhub.NewDeliveryClient(websubhub.Subscription{
    Hub:      "https://hub.example.com/websub",
    Mode:     websubhub.ModeSubscribe,
    Topic:    "https://publisher.example.com/topics/orders",
    Callback: callbackURL,
    Secret:   secret,
}, websubhub.DeliveryConfig{HTTPClient: outboundClient})
if err != nil {
    return err
}

response, err := client.Deliver(ctx, websubhub.ContentDistribution{
    ID:          messageID,
    ContentType: "application/json",
    Body:        payload,
})
```

`Deliver` performs one attempt for the hub, topic, callback, and secret in
the supplied subscription. It preserves the complete content type, including
parameters. Applications decide retry, acknowledgment, dead-lettering, and
stale-subscription policy. An HTTP 410 error matches
`websubhub.ErrSubscriptionGone`.

## Publisher extension

Set `Config.EnablePublisherExtension` and supply all three topic/update
callbacks to accept the optional publisher extension. `PublisherClient`
supports register, deregister, content publish, and event-only notify. This
extension uses `X-Go-Publisher` to distinguish content publication from an
event-only notification and is separate from the W3C WebSub conformance claim.

Use `AddDiscoveryLinks` to append WebSub `rel=self` and `rel=hub` response links.
As a standards-facing discovery helper, it requires absolute HTTP(S) URLs.

## Security model

The package deliberately has no authentication or TLS configuration abstraction:

- authenticate and authorize inbound requests with ordinary `net/http`
  middleware;
- configure server TLS on `http.Server`;
- inject a restricted `http.Client` for callback verification, delivery, and
  publisher calls when custom roots, client certificates, proxies, or SSRF
  controls are required;
- do not put sensitive query values in callback URLs;
- treat every application callback as concurrency-safe.

Outbound clients refuse redirects, bound response bodies, and never mutate a
caller-supplied `http.Client`.

Read [`SECURITY.md`](SECURITY.md) before exposing a hub to untrusted networks.

## Verification

```sh
go test ./...
go vet ./...
go test -race ./...
go test -cover ./...

cd examples/in-memory-websubhub
go test ./...
go vet ./...
```

The initial suite covers 85% or more of statements and includes protocol,
publisher-extension, TLS, authentication-middleware, bounded-body, lifecycle,
and backpressure behavior.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).

Copyright 2026 Ayesh Almeida.
