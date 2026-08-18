# Repository guidance

## Purpose and status

`lib-websubhub` is a thin WebSub hub protocol framework for Go. The repository
root is the published `github.com/ayeshLK/lib-websubhub` module and the package
name is `websubhub`.

The API is pre-release and under active validation. Compatibility is not yet
guaranteed, but an agent must not make an unrelated breaking change without
explicit task scope. Deliberate API changes require coordinated specification,
test, documentation, and `CHANGELOG.md` updates.

This package is not a complete hub product and must not claim standalone W3C hub
or subscriber conformance while the gaps in `docs/spec.md` remain.

## Authority and reading order

Before changing code, read the documents relevant to the task:

1. `README.md` for the public project overview and basic usage.
2. `docs/spec.md` for the normative API, protocol behavior, ownership boundary,
   security limitations, and W3C gap analysis.
3. `docs/testing-plan.md` for required cases and the coverage gate.
4. `SECURITY.md` and specification Section 14 for security-sensitive work.
5. `docs/release-plan.md` for Go compatibility, CI, versioning, or release work.
6. The README and tests of each affected example module.

`docs/spec.md` is the repository's normative framework contract. If code, tests,
and the specification disagree, do not silently choose one: identify the
conflict and update the intended contract, implementation, tests, and public
documentation together.

Keep documentation self-contained. Do not add historical migration baselines or
references to unrelated implementations unless the user explicitly requests
them.

## Repository map

- `handler.go`: inbound HTTP parsing, dispatch, verification queue, and handler
  lifecycle.
- `service.go`: application callbacks, modes, metadata, and result types.
- `subscription.go`: subscription and unsubscription protocol types.
- `challenge.go`: intent-verification callback construction and execution.
- `delivery.go`: one-attempt content delivery to one subscriber.
- `publisher.go`: optional publisher-to-hub extension client.
- `discovery.go`: WebSub discovery Link-header helpers.
- `errors.go`: sentinel and typed errors.
- `helpers.go`: shared validation, cloning, body, URL, and HTTP helpers.
- `*_test.go`: unit, wire-level, lifecycle, security-boundary, and conformance
  tests for the root module.
- `examples/in-memory-websubhub/`: runnable hub in an independent Go module.
- `docs/`: specification and implementation, testing, documentation, and
  release plans.
- `.github/workflows/`: pull-request validation and manual release automation.

Examples are independent modules so future example-only dependencies do not
enter the core module graph. Run and tidy each affected example from its own
directory.

## Architecture and ownership boundary

The framework owns only protocol mechanics:

- bounded HTTP parsing and response mapping;
- subscription and unsubscription admission, validation, and verification;
- typed application callbacks and immutable snapshots;
- bounded asynchronous verification work and cancellation;
- one logical content-delivery attempt to one subscriber;
- discovery helpers and the optional publisher extension.

Applications own:

- topic and verified-subscription persistence;
- atomic renewal, unsubscription, and mandatory lease expiry;
- content selection, fan-out, ordering, retry, acknowledgement, and DLQ policy;
- brokers, databases, clustering, replication, and node ownership;
- TLS termination, authentication, authorization, quotas, and rate limiting;
- callback and topic SSRF policy, observability, and deployment lifecycle.

Do not add storage, broker abstractions, automatic retry, fan-out, schedulers,
authentication frameworks, or TLS configuration abstractions to the core API
without an explicit architecture decision that changes this boundary.

Do not add a compiler plugin, code generator, reflection-based callback
discovery, or custom analyzer for invariants already enforced by Go type checking
and `NewHandler` validation.

## Protocol invariants

Preserve these behaviors unless the specification is deliberately revised:

- Successful subscription and unsubscription admission returns exactly
  `202 Accepted`; asynchronous validation and verification occur after the
  response is committed.
- Verified subscription and unsubscription callbacks are required. Application
  state changes occur only after successful verification.
- Topics, hubs, and callbacks are absolute HTTP(S) URLs. Deployment-specific
  canonicalization and destination policy remain application responsibilities.
- Existing callback `RawQuery` bytes are preserved exactly and newly encoded
  verification parameters are appended without normalizing existing values.
- Outbound verification and delivery refuse redirects. An application admission
  callback may intentionally return a supported 307 or 308 redirect.
- Request and response bodies, verification concurrency, and queue capacity are
  bounded. Queue saturation must not be reported as accepted work.
- Content delivery preserves exact body bytes and the complete `Content-Type`,
  including parameters, and performs only one attempt.
- `LeaseSeconds`, `EffectiveLeaseSeconds`, and `Secret` remain strings matching
  their form representation. Secrets must be fewer than 200 bytes and must not
  appear in logs or errors.
- An unsubscription lease value is accepted and ignored; it is not exposed as
  active lease state.
- Lease expiry is application-owned. The framework supplies verification timing
  metadata but does not persist or expire subscriptions.
- Delivery Link metadata is currently derived from the subscription snapshot.
  This is a documented conformance limitation, not an accidental guarantee.

When changing wire behavior, assert literal methods, status codes, headers,
queries, media types, and bytes in tests. Do not test only through a constant
that could change together with an incorrect implementation.

## Concurrency and lifecycle invariants

- Application callbacks may run concurrently and must receive cloned mutable
  data rather than request-owned maps or slices.
- Accepted verification work is bounded and accounted for exactly once.
- `Handler.Close` stops admission, drains accepted work when possible, respects
  its context deadline, cancels remaining work, and is safe to call repeatedly.
- Propagate cancellation and deadlines through outbound calls. Do not create
  unbounded goroutines, queues, response reads, or retry loops.
- Avoid timing sleeps in tests. Use channels, barriers, contexts, and bounded
  deadlines to prove ordering and shutdown behavior.

## Security boundary

- Treat syntactic HTTP(S) validation as distinct from SSRF protection. A
  production application must enforce destination policy at dial time with an
  appropriate transport or network boundary.
- Use ordinary `net/http` middleware for inbound authentication and
  authorization. Configure TLS on `http.Server` and outbound trust on injected
  `http.Client` values.
- Preserve redirect refusal, bounded bodies, safe response headers, secret-safe
  errors, and non-mutating handling of caller-supplied HTTP clients.
- Never log secrets, Authorization values, callback capability queries, or
  payload bodies by default.
- Do not weaken secure defaults for convenience. Document any transferred
  responsibility explicitly in `SECURITY.md` and `docs/spec.md`.

## Publisher extension

Publisher-to-hub notification is not standardized by WebSub. This project has
an optional extension, disabled by default:

- `Config.EnablePublisherExtension` enables register, deregister, content
  publication, and event-only notification modes.
- All three publisher callbacks are required when the extension is enabled.
- `X-Go-Publisher: publish|event` selects content or event notification mode.
- `HeaderGoPublisher` is the exported header-name constant.
- Extension behavior is excluded from the W3C conformance claim and must remain
  isolated from subscription and unsubscription paths.

## Dependencies and compatibility

- The core module and its tests use only the Go standard library. Confirm with
  `go list -m all`; the output must contain only this module.
- The minimum Go version is declared by `go.mod` and is currently Go 1.25. Do not
  raise it solely because a newer toolchain is locally installed.
- CI validates Go 1.25.x and 1.26.x, with the newest supported version receiving
  race and coverage checks.
- Releases use ordinary root-module semantic-version tags such as `v0.1.0`; do
  not use subdirectory tag prefixes.
- External dependencies require explicit architecture and security review.

## Change workflow

Before editing:

- inspect `git status` and preserve all existing user changes;
- locate affected tests and specification sections;
- determine whether the change affects the public API, wire behavior, security
  boundary, example modules, or release process.

For implementation changes, run from the repository root:

```sh
gofmt -w *.go
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

Statement coverage must remain at least 85%, but coverage alone does not replace
the protocol-critical matrix in `docs/testing-plan.md`. If race testing cannot
run because CGO or a C compiler is unavailable, report that limitation rather
than claiming it passed. Do not commit `coverage.out`.

For documentation-only changes, run `git diff --check`, verify local links and
commands, and run tests when code snippets, exported names, module paths, or
documented behavior changed.

For the in-memory example, run from `examples/in-memory-websubhub/`:

```sh
gofmt -w *.go
go vet ./...
go test ./...
```

For API, protocol, or security changes:

- update `docs/spec.md`, relevant tests, README usage, and `CHANGELOG.md` in the
  same change;
- update `docs/testing-plan.md` when the behavior matrix changes;
- add table-driven boundary cases and a regression test for every fixed defect;
- test enabled and disabled publisher-extension configurations when relevant.

For workflow or release changes, keep third-party actions pinned to full commit
SHAs, retain least-privilege permissions, and validate YAML syntax. Do not create
tags, releases, commits, or pushes unless the user explicitly requests them.

## Documentation and licensing

- Every exported identifier requires an accurate Go doc comment.
- Public examples must compile or clearly identify omitted setup.
- Keep README, specification, tests, and implementation terminology aligned.
- New Go files must carry the Apache-2.0 source header with
  `Copyright 2026 Ayesh Almeida`.
- Keep the standard Apache-2.0 `LICENSE` text unmodified. A separate `NOTICE`
  file is not used; add one only if a reviewed attribution requirement makes it
  necessary.

## Repository hygiene and completion

- Do not commit credentials, generated binaries, build artifacts, coverage
  profiles, local Go workspaces, editor state, or large reference archives.
- Do not expose secrets or sensitive payloads in commands, tests, fixtures,
  logs, errors, or documentation.
- Run `git diff --check` and inspect `git status` before handoff.
- A change is complete only when code, tests, documentation, module boundaries,
  and observable behavior agree and all applicable validation has passed or an
  explicit validation limitation has been reported.
