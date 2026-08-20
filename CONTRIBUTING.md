# Contributing

Thank you for helping improve `lib-websubhub`.

## Development setup

Install Go 1.25 or newer. Run framework commands from the repository root and
example commands from each example directory. The core module and its tests must
remain standard-library-only; external dependencies require architecture and
security review.

## Validate changes

Run the complete core suite before opening a pull request:

```sh
gofmt -w *.go
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

Statement coverage must remain at least 85%. Pull requests must not reduce total
coverage and must cover at least 85% of changed lines. CI reports both conditions
through Codecov. Do not commit `coverage.out`.

Validate an affected example from its independent module:

```sh
cd examples/in-memory-websubhub
gofmt -w *.go
go vet ./...
go test ./...

cd ../kafka-websubhub
gofmt -w *.go
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
docker compose config --quiet
```

Kafka example tests use an in-process broker double and do not require Docker.

## Testing expectations

Use table-driven tests for boundary cases and add a regression test for every
fixed defect. For wire behavior, assert literal methods, status codes, headers,
queries, media types, and body bytes. Cover enabled and disabled publisher
extension configurations when relevant.

Avoid timing sleeps in concurrency tests. Use channels, barriers, contexts, and
bounded deadlines to prove ordering, cancellation, and shutdown behavior.
Coverage does not replace protocol-critical behavioral assertions.

## API, protocol, and security changes

`docs/spec.md` is the normative framework contract. An API, protocol, or security
change must update the specification, implementation, tests, README usage, and
`CHANGELOG.md` together. Every exported identifier requires an accurate Go doc
comment. Read `SECURITY.md` before changing trust boundaries, outbound HTTP,
secret handling, or resource limits.

## Commits and pull requests

Use Conventional Commit subjects. `fix:` selects a patch release, `feat:`
selects a minor release, and `type!:` or a `BREAKING CHANGE:` footer marks an
incompatible change.

Pull requests should explain the observable change, identify affected protocol
or security behavior, link relevant issues, and list validation performed. Keep
commits focused and exclude unrelated artifacts.

## Releases

The root module uses immutable semantic-version tags such as `v0.5.0`. Do not
create, move, reuse, or delete release tags manually.

A maintainer starts the Release Please workflow to prepare a version and
changelog pull request. After that pull request is reviewed and merged, a
maintainer starts the publication workflow through the `release` environment,
which must require maintainer approval. Publication validates the module and
examples before creating the GitHub Release. Workflow changes must preserve
least-privilege permissions and pin third-party actions to full commit SHAs.

## Repository hygiene and licensing

Do not include credentials, generated binaries, coverage profiles, dependency
caches, editor state, or unrelated test artifacts. New Go files must carry the
Apache-2.0 source header with `Copyright 2026 Ayesh Almeida`.

Unless explicitly stated otherwise, contributions intentionally submitted to
this project are licensed under Apache-2.0, as described by section 5 of
`LICENSE`.
