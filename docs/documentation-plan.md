# Documentation plan

Documentation must make the thin framework boundary unmistakable: the package
handles WebSub-over-HTTP, while applications decide how topics, subscriptions,
messages, delivery policy, and distributed state work.

## 1. Audiences

- **Hub application developer:** implements `Service` callbacks and application
  state.
- **Broker-backed implementer:** connects verified callbacks, publisher updates,
  consumers, and `DeliveryClient` to a message broker.
- **Publisher author:** advertises discovery links and uses `PublisherClient`.
- **Framework maintainer:** needs protocol traceability, callback contracts,
  test commands, security boundaries, and release rules.
- **Operator:** configures the surrounding HTTP server, TLS, authentication,
  observability, health endpoints, and application lifecycle.

## 2. Required documents

### Root README

Include:

- the thin-protocol-layer value proposition;
- the standard-library-only promise;
- installation and support matrix;
- a minimal `Service` and `NewHandler` example;
- a single-subscriber `DeliveryClient` example;
- links to customization, conformance, security, and operational guides;
- a clear list of application responsibilities.

### Package documentation

`doc.go` explains:

- which HTTP mechanics the framework owns;
- which state and delivery decisions the application owns;
- callback order and concurrency;
- required and optional `Service` fields;
- handler and client lifecycle;
- publisher and hub-error extension boundaries;
- typed error inspection.

Every exported identifier receives a useful Go doc comment.

### Callback reference

Document every callback with:

- when it runs;
- whether it is synchronous or asynchronous;
- its default when nil;
- allowed `Result` and error forms;
- concurrency and cloning guarantees;
- whether a returned error affects the already-sent HTTP response;
- the exact next callback in the lifecycle.

Include sequence diagrams for subscription, denial, external verification,
verified callback failure, and unsubscription.

### Minimal hub guide

Build an application-owned in-memory example:

1. create topic and subscription maps in the example;
2. implement registration and validation callbacks;
3. persist state only inside verified callbacks;
4. mount `Handler` on `http.ServeMux`;
5. configure `http.Server` timeouts and TLS;
6. deliver one update using an application loop and `DeliveryClient`;
7. shut down the server, handler, and application workers in ownership order.

The maps are example application code, not framework types.

### Broker-backed architecture guide

Use the reviewed production hub as the architecture profile:

```text
verified callbacks -> broker administrator -> state event
update callback    -> broker producer
state consumer     -> application cache and node ownership
broker consumer    -> DeliveryClient -> ack / nack / DLQ
```

Explain topic provisioning, subscription consumer identity, snapshots plus state
events, node affinity, stale subscriptions, 410 deletion, and retry/DLQ policy.
Use fake interfaces in executable examples; vendor adapters remain external.

### Delivery guide

Explain:

- constructing a client from a verified subscription;
- exact-byte HMAC behavior;
- Link headers;
- reserved-header safety;
- 2xx, 410, and other failure classification;
- why the client performs one attempt;
- how an application may implement retry, broker acknowledgment, dead-lettering,
  stale state, and deletion.

### Publisher guide

Separate WebSub discovery advertisement from publisher-to-hub notification.
Explain that publisher-to-hub notification transport is not standardized,
identify the optional project extension, and show contexts and typed errors.

### Configuration reference

List every default and validation rule. Distinguish handler verification settings
from delivery and publisher client settings. Explain that nil callback defaults
are intentional, while verified callbacks are required.

### Conformance matrix

For each applicable W3C requirement, state:

- framework-owned or application-owned;
- implementation status;
- test requirement tag;
- extension notes where applicable.

Keep extension behavior clearly separated from the W3C conformance matrix.

### Security guide

Cover:

- callback SSRF and DNS rebinding;
- body, timeout, queue, and concurrency limits;
- TLS trust and transport injection;
- secret and Authorization handling;
- exact-byte signing;
- redirect refusal;
- response-header validation;
- middleware authentication and authorization;
- application responsibilities when external verification is enabled.

### Contributor and release guides

Include repository layout, formatting, tests, fuzz corpus, public API review,
zero-dependency validation, supported Go versions, changelog rules, security
reporting, and the release checklist.

## 3. Examples

```text
examples/
    minimal-hub/
    custom-topic-policy/
    external-verification/
    publisher/
    signed-delivery/
    broker-backed-pattern/
    tls-hub/
    graceful-shutdown/
    publisher-extension/
```

Examples must:

- use ephemeral ports in tests;
- configure HTTP server timeouts;
- close response bodies;
- propagate contexts;
- handle signals where appropriate;
- separate framework code from application-owned state;
- contain no third-party imports.

The broker-backed example uses a small fake broker defined inside the example. It
demonstrates composition, not a new framework abstraction.

## 4. Documentation validation

CI runs:

- `go test ./...` so examples compile and execute;
- `go vet ./...`;
- exported-identifier documentation checks;
- local Markdown-link checks;
- guide snippet compilation where snippets are not examples;
- conformance-tag checks mapping requirements to tests.

Before release, manually verify rendered package docs, quickstarts on a clean
environment, callback sequencing, supported Go versions, defaults, ownership
language, and security warnings.

## 5. Milestones

| Implementation phase | Documentation due |
|---|---|
| API skeleton | README, `doc.go`, callback reference outline, API decisions |
| parser/handler | HTTP contract, `Result` mapping, errors, configuration |
| verification | subscription sequences, controller, denial/hub-error behavior |
| delivery client | HMAC and application retry/ack/DLQ guide |
| publisher extension | discovery, publisher guide, extension behavior matrix |
| release candidate | broker-backed profile, security, conformance, operations |

Documentation completeness is a release gate equal to testing and API review.
