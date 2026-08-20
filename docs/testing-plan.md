# Testing plan

## 1. Validation strategy

Go needs no compiler plugin for this API:

- callback functions are statically assignable only to their declared function
  types;
- invalid callback signatures fail normal Go compilation;
- `NewHandler` checks required callback presence and configuration-dependent
  combinations;
- compile-time examples and ordinary constructor tests cover the remaining API
  contract.

A custom analyzer is explicitly out of scope unless a future declarative API
introduces invariants that compilation and constructor validation cannot express.

## 2. Coverage target

The release gate requires:

- at least **85% statement coverage** for the framework package;
- no reduction in total project coverage relative to the pull request base;
- at least **85% coverage** of lines changed by a pull request;
- complete coverage of the protocol-critical matrix below;
- complete coverage of the documented handler, client, and extension behavior;
- race, fuzz, cancellation, and lifecycle testing.

Application examples are tested for composition, but application storage,
brokers, retries, and clustering are not included in the framework coverage
percentage.

## 3. Test layers

### Unit tests

Table-driven tests cover media types, form values, URLs, lease selection,
callback result mapping, Link construction, HMAC, error wrapping, and delivery
response classification.

### Handler tests

Use `httptest.NewRequest` and `httptest.ResponseRecorder` for parsing and
dispatch. Use `httptest.Server` when response timing, callback sequencing,
redirect handling, query preservation, or cancellation matters.

Record callback invocations in channels so tests prove order without sleeps.

### Client tests

Test `DeliveryClient` and `PublisherClient` against `httptest.Server` and
`httptest.NewTLSServer`. Assert exact method, URL, query, headers, body bytes,
signature, response mapping, and body closure.

### Integration tests

Compose:

- hub handler;
- application-owned topic and subscription maps;
- subscriber callback;
- application delivery loop using `DeliveryClient`;
- publisher using `PublisherClient`.

Exercise register -> subscribe -> validate -> verify -> application storage ->
application delivery -> unsubscribe. These examples prove flexibility without
making the framework own the state.

The Kafka-backed example compiles a real broker adapter and tests producer,
consumer-owned state projection, eventual consistency, replay, acknowledgement,
checkpoint-free snapshots, bounded replay through a captured log end,
greatest-revision selection across polled snapshot records,
hashed topic and consumer-group mapping, server-owned subscription worker
selection across hub instances, JSON content validation, bounded retry, stale-subscription state, and
all-success batch commits through an in-process broker double. Its documented
Compose profile enables optional manual testing against Apache Kafka.

### Concurrency and lifecycle tests

Run all packages under `go test -race`. Cover:

- concurrent handler calls;
- queue saturation;
- callback mutation after return;
- validation and verification cancellation;
- callback panic recovery;
- close while work is pending;
- repeated close;
- calls after close;
- no leaked verification workers.

The framework does not test application-state ordering because it does not own
application state.

### Fuzz tests

Fuzz form bodies, media types, percent encoding, URL/query preservation, lease
values, Link construction, publisher responses, result headers, and signature
inputs. Every fixed parser or security regression becomes a seed.

## 4. Protocol-critical matrix

| Area | Required cases |
|---|---|
| Method | POST; all unsupported methods return 405 and `Allow` |
| Media type | form type; parameters; UTF-8; missing/malformed/unsupported values |
| Form input | missing, empty, duplicate, unknown, malformed escaping, oversized |
| Modes | subscribe/unsubscribe; extension enabled/disabled; invalid |
| URLs | HTTP/HTTPS; relative; scheme; userinfo; fragment; unreserved percent-decoding; reserved-escape and overlapping callback-query preservation |
| Lease | omitted/default; positive; zero; negative; non-decimal; overflow; cap |
| Secret | omitted; 199 bytes; 200 rejected; cloning; never logged |
| Dispatch | correct callback and typed message; nil optional callback; required callback validation |
| Result | defaults; custom status/header/body; invalid status; unsafe header |
| Admission | reject; redirect 307/308; queue admission; queue-full 503 |
| Ordering | 202 written before validation and verification |
| Validation | allow; denied callback; unexpected failure; nil callback |
| Challenge | allowed alphabet; uniqueness; correct query; subscribe lease; unsubscribe omission |
| Verification | each 2xx; exact/wrong body; 3xx/4xx/5xx; timeout; oversized body; redirect refused |
| Controller | disabled error; enabled mark; duplicate mark; verified callback invocation |
| Verified callback | only after success; receives clones; failure and optional hub-error callback |
| Delivery wire | exact bytes/type; Link values; safe headers; no framework-defined extension headers |
| HMAC | known SHA-256 vectors; exact body; lowercase hex; absent without secret |
| Delivery result | every 2xx; 410 sentinel; other statuses; timeout; network error; bounded body |
| Publisher client | register/deregister/publish/notify success and detailed errors |
| TLS | custom roots; successful TLS; invalid trust; no insecure default |
| Lifecycle | close drains; deadline aborts; idempotence; calls after close |
| Error safety | `errors.Is/As`; bounded metadata; no secrets/auth/payload in logs |

## 5. Runtime behavior coverage

| Behavior group | Go coverage |
|---|---|
| Core hub requests | handler parsing, callback dispatch, validation, verification, and update modes |
| Publisher client | success and failure mapping for registration, deregistration, publish, and notify |
| Delivery client | payload types, exact wire behavior, signatures, and response mapping |
| TLS | handler and client tests with custom roots |
| Authentication boundary | middleware composition and request-header propagation |
| External verification | explicitly enabled `Controller.MarkVerified` behavior |
| Configuration | `Config` defaults and validation |
| Immutability | cloning and concurrent mutation protection |
| Errors and headers | `Result` mapping and typed HTTP error details |
| Subscriber notifications | denial and optional hub-error callback behavior |
| Utilities | headers, bodies, media types, Link, and HMAC helpers |

Go table tests may represent many cases in one function. Every mapped behavior
must have explicit assertions.

## 6. Compile-time API checks

Use ordinary Go compilation:

```go
var register RegisterTopicFunc = func(
    context.Context,
    TopicRegistration,
    RequestMetadata,
) (Result, error) {
    return Result{}, nil
}
```

Examples compile as part of `go test`. Constructor tables check nil required
callbacks and extension-dependent combinations. There is no plugin-specific test
suite.

## 7. W3C traceability

Maintain requirement tags mapping each framework-owned W3C MUST to tests:

- request encoding and required fields;
- 202 before asynchronous outcome;
- denied callback;
- intent verification and challenge echo;
- finite effective lease;
- callback query preservation;
- authenticated single-subscriber delivery;
- full body and matching content type as supplied by the application;
- hub/self Link headers;
- delivery result classification.

The conformance matrix separately lists application responsibilities such as
verified-state persistence, lease expiry, complete-content selection, retry, and
continued delivery attempts.

## 8. CI commands and gates

```sh
gofmt -w .                         # CI checks for no diff
go vet ./...
go test ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

CI enforces the absolute 85% statement floor before uploading `coverage.out` to
Codecov through GitHub OIDC. The blocking `codecov/project` status compares total
coverage with the pull request base and permits no decrease. The blocking
`codecov/patch` status requires at least 85% coverage of changed lines. The
default-branch ruleset must require `Coverage`, `codecov/project`, and
`codecov/patch`.

Scheduled jobs run bounded fuzz campaigns and cross-platform tests on the two
supported Go major versions.

No release is allowed with:

- statement coverage below 85%;
- total coverage lower than the pull request base;
- changed-line coverage below 85%;
- an uncovered protocol-critical matrix row;
- race reports, leaked workers, or timing-sleep-dependent tests;
- an external core-module dependency;
- undocumented exported identifiers;
- unresolved high-severity security findings.
