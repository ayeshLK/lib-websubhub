# Contributing

Thank you for helping improve `lib-websubhub`.

## Development setup

Install Go 1.25 or newer. The core module must remain standard-library-only.
Run framework commands from the repository root and example commands from each
example's own module directory.

Before opening a pull request:

```sh
gofmt -w .
go vet ./...
go test -shuffle=on ./...
go test -race ./...

cd examples/in-memory-websubhub
gofmt -w .
go vet ./...
go test ./...
```

Do not add a third-party dependency to the core module without an explicit
architecture and security review. API or protocol changes must update
`docs/spec.md`, relevant tests, and user-facing documentation in the same pull
request.

Use focused commits and explain observable behavior changes. Do not include
credentials, generated binaries, coverage profiles, dependency caches, or
unrelated test artifacts.

Unless explicitly stated otherwise, contributions intentionally submitted to
this project are licensed under Apache-2.0, as described by section 5 of
`LICENSE`.
