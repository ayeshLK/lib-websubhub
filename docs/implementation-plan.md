# Implementation plan

The implementation is intentionally a thin WebSub-over-HTTP layer. Each phase
must avoid introducing application state, broker abstractions, fan-out, or
delivery scheduling into the core package.

## Phase 0: repository and API skeleton

Deliver:

- use module path `github.com/ayeshLK/lib-websubhub` and Apache-2.0;
- initialize `go.mod` with Go 1.25 and no requirements;
- define protocol messages, `Service` callback types, `Config`, `Result`, and
  typed errors;
- define `Handler`, `DeliveryClient`, and `PublisherClient` signatures;
- add compile-time examples showing valid callback assignment;
- configure format, vet, race, and coverage CI.

Constructor validation enforces callback and configuration invariants:

- verified subscription and unsubscription callbacks are required;
- publisher callbacks are required only when the publisher extension is enabled;
- invalid callback combinations and configuration return errors;
- Go's compiler rejects functions with incompatible signatures.

There is no compiler plugin, code generator, reflection-based method discovery,
or custom analyzer in this phase.

Exit criteria:

- `go list -deps` shows only the module and standard library;
- `go test ./...`, `go vet ./...`, and `go test -race ./...` pass;
- the public callback API and defaults complete architecture review.

## Phase 1: bounded HTTP parsing and dispatch

Implement:

- method and media-type validation;
- bounded form and arbitrary update-body parsing;
- strict required/single-value parameter extraction;
- URL, lease, secret, charset, and unknown-parameter handling;
- immutable `RequestMetadata` snapshots;
- dispatch for subscribe, unsubscribe, and optional publisher modes;
- `Result` and typed-error to HTTP response mapping;
- panic containment around application callbacks.

Use `http.MaxBytesReader`, `mime.ParseMediaType`, `url.ParseQuery`,
`strconv.ParseInt`, and explicit cloning. Do not pass the live
`http.ResponseWriter` or mutable request body to application callbacks.

Exit criteria: parsing tables, callback dispatch tests, result mapping, body
limits, and initial fuzz seeds pass.

## Phase 2: subscription protocol orchestration

Implement the ordered pipeline:

```text
initial callback
    -> bounded queue admission
    -> HTTP 202
    -> asynchronous validation callback
    -> denied callback OR intent verification
    -> verified application callback
```

Include:

- fixed verification worker pool and bounded queue;
- `crypto/rand` single-use challenges;
- callback-query append semantics;
- redirect refusal, response limits, and exact challenge comparison;
- subscription lease selection;
- subscription and unsubscription flows;
- `Controller.MarkVerified` guarded by explicit configuration;
- denial and optional hub-error callbacks;
- callback cancellation and idempotent `Handler.Close`.

The verified callback is the end of framework responsibility. Do not persist,
deduplicate, renew, or delete subscription state in the package.

Exit criteria: sequencing tests prove that 202 precedes validation, failed
verification never invokes the verified callback, queue overflow is reported,
and shutdown drains accepted protocol work.

## Phase 3: single-subscriber delivery client

Implement:

- immutable subscription and delivery snapshots;
- exact body and content-type transmission;
- `rel=hub` and `rel=self` Link headers;
- HMAC-SHA256 over exact transmitted bytes;
- safe application-header copying;
- outbound context/deadline behavior;
- redirect refusal and bounded response processing;
- 2xx success, distinct 410, and typed other-status errors.

The client performs one logical delivery attempt. Do not add retries, worker
pools, subscription enumeration, acknowledgment, DLQ, or automatic state
deletion.

Exit criteria: wire-level golden tests, known HMAC vectors, TLS tests, response
classification, cancellation, and no-alias tests pass.

## Phase 4: publisher extension

Implement:

- discovery Link-header advertisement;
- publisher client register, deregister, publish, and notify methods;
- optional inbound register, deregister, update, and event modes;
- the private `X-Go-Publisher` request header;
- accepted/denied form response handling;
- bounded typed client errors preserving safe status/header/body details.

Keep extension parsing isolated from core subscription paths. Disabling the
extension must remove those modes without affecting WebSub behavior.

Exit criteria: publisher-extension scenarios pass with the extension enabled,
and core tests pass in both enabled and disabled configurations.

## Phase 5: hardening and release candidate

Complete:

- parser, URL, callback, Link, response, and signature fuzzing;
- race, cancellation, queue-saturation, panic, and shutdown testing;
- threat-model review for SSRF, DNS rebinding, slow peers, body exhaustion,
  response splitting, secret leakage, and goroutine exhaustion;
- public API compatibility snapshot;
- documentation, conformance matrix, adoption guide, and examples;
- a broker-backed architecture example using fake application interfaces;
- release checklist and changelog.

## Dependency direction

```text
Handler
    -> protocol parser
    -> Service callbacks
    -> verification coordinator
        -> outbound HTTP

DeliveryClient
    -> outbound HTTP
    -> Link and HMAC helpers

PublisherClient
    -> outbound HTTP
    -> extension codecs
```

No arrow points to application storage, message brokers, delivery workers, or
cluster state.

## Application integration profile

The documentation must show how a production application composes the thin
framework:

```text
Service verified callbacks
    -> application subscription manager
        -> broker administrator
        -> state-event producer

Service update callback
    -> application content producer

Application subscription supervisor
    -> broker consumer
    -> DeliveryClient
    -> application ack / nack / DLQ policy
```

Kafka, JMS, Solace, databases, JWT, and telemetry adapters remain outside this
module and may use their normal third-party dependencies.

## Review gates

1. **Boundary gate:** no application-state or broker policy in the core.
2. **API gate:** callbacks, required combinations, defaults, and error mapping.
3. **Protocol gate:** W3C traceability and extension behavior coverage.
4. **Concurrency gate:** bounded verification, cancellation, and shutdown.
5. **Security gate:** outbound callback and resource-limit review.
6. **Release gate:** tests, docs, examples, and zero external dependencies.
