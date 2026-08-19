# Release and CI/CD plan

This document defines how the Go WebSubHub module is published, validated, and
maintained. Protocol behavior and its release gates remain defined by the
[framework specification](spec.md) and [testing plan](testing-plan.md).

## 1. Distribution model

Go modules use decentralized source distribution. The Git repository is the
authoritative source; there is no registry to which this project uploads a
library archive.

The module lives at the repository root and declares its permanent path:

```go
module github.com/ayeshLK/lib-websubhub

go 1.25
```

The project is licensed under Apache-2.0. Moving the repository later does not
automatically change existing import paths.

A public release is made by creating an immutable semantic-version tag. Go's
public module proxy can then cache the source, the checksum database records its
content hash, and pkg.go.dev can render package documentation:

```text
GitHub source
    -> vX.Y.Z tag
    -> proxy.golang.org
    -> sum.golang.org
    -> pkg.go.dev
```

Consumers install a release with:

```sh
go get github.com/ayeshLK/lib-websubhub@v0.1.0
```

A published tag must never be moved, deleted and reused, or repointed. If a
version is defective, publish a corrected version. If consumers must not select
it, add a `retract` directive in a later `go.mod` release.

References:

- [Publishing a Go module](https://go.dev/doc/modules/publishing)
- [Go modules reference](https://go.dev/ref/mod)
- [pkg.go.dev license policy](https://pkg.go.dev/license-policy)

## 2. Versioning policy

The module follows semantic versioning.

- Use `v0.x` while the public API is being validated.
- Use pre-releases such as `v0.1.0-alpha.1` for early integration testing.
- Treat `v1.0.0` as the stable public API commitment.
- Require a new major version for incompatible exported API or semantic changes.
- Append the major version to the module path from v2 onward, for example
  `github.com/ayeshLK/lib-websubhub/v2`.
- Describe protocol-tightening changes, minimum-Go changes, and security fixes
  explicitly in release notes.

GitHub Releases provide human-readable notes and provenance around the tag, but
the Go module itself is the tagged repository source. The project publishes no
binaries, containers, or generated library archives unless a future deliverable
creates a separate need.

## 3. Go compatibility policy

The initial module declares Go 1.25 as its minimum and performs primary
development on Go 1.26. It supports the latest stable Go release and its
immediate predecessor when a framework version is released.

There are no separate WebSubHub releases, source branches, or artifacts for Go
1.25 and Go 1.26. A single module version is source-compatible with both.

Validation is deliberately asymmetric to keep the cost small:

| Toolchain | Required validation |
|---|---|
| minimum supported, initially Go 1.25.x | Linux build, vet, and ordinary tests |
| latest stable, initially Go 1.26.x | full tests, race detector, coverage, examples, and security checks |
| latest stable on macOS and Windows | build and smoke tests |
| next unreleased Go toolchain | optional scheduled, non-blocking signal |

The `go` directive expresses the minimum version actually required by the
source. It must not be raised merely because maintainers use a newer toolchain.
A minimum-version increase is justified only by a required language feature,
standard-library API, correctness fix, or security policy.

When a new stable Go version is released:

1. add it through a compatibility pull request;
2. run the full suite and review relevant `net/http`, `crypto/tls`,
   `crypto/x509`, runtime, and toolchain changes;
3. make it the primary CI toolchain;
4. retain its predecessor as the lightweight compatibility target;
5. raise the `go` directive only in a planned non-patch framework release;
6. document the removed Go version in release notes.

The compatibility window changes through a reviewed release, not automatically
on the day a Go version is announced.

## 4. Continuous integration

GitHub Actions is the CI/CD platform. Pull-request workflows use no repository
secrets and default to read-only permissions:

```yaml
permissions:
  contents: read
```

Workflows pin external actions to full commit SHAs. A comment beside each SHA
records its release tag, and Dependabot proposes updates. Concurrency
cancellation lets a newer commit supersede obsolete work on the same pull
request.

### 4.1 Pull-request workflow: `ci.yml`

Run on every pull request and push to the default branch:

1. **Repository policy**
   - require no `gofmt` diff;
   - run `go mod tidy` and require no diff;
   - verify the module graph contains no external module;
   - validate local documentation links and required license files.
2. **Minimum Go compatibility**
   - use Go 1.25.x on Linux;
   - run vet, build, and ordinary tests.
3. **Primary validation**
   - use Go 1.26.x on Linux;
   - run vet, build, ordinary tests, race tests, and coverage;
   - enforce at least 85% framework statement coverage and every
     protocol-critical testing-matrix row.
4. **Platform smoke tests**
   - use Go 1.26.x on macOS and Windows;
   - build all packages and execute platform-independent tests and examples.

The standard command set is:

```sh
go vet ./...
go build ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

The standard-library-only promise applies to implementation and tests. CI tools
may be external, but they must be version-pinned and must not add requirements
to the published module graph.

### 4.2 Fuzz workflow: `fuzz.yml`

Run bounded fuzz campaigns on the latest stable Go version weekly, manually
through `workflow_dispatch`, and for a short duration when protocol parsers
change. Targets cover forms, URLs, media types, Link headers, verification
responses, HMAC inputs, malformed encodings, and size boundaries. Long-running
fuzzing does not block every pull request; promoted regression inputs do.

### 4.3 Security automation

Enable:

- CodeQL default setup for Go;
- `govulncheck` on a schedule and before release;
- GitHub secret scanning and push protection;
- Dependabot updates for GitHub Actions;
- dependency review if an external dependency is ever proposed.

An external dependency is a design-policy change requiring architectural and
security review. It cannot be introduced by an automated dependency update.

## 5. Continuous delivery and release workflow

Release preparation and publication are separate. `release-please.yml` runs on
updates to the default branch and maintains a release pull request from
Conventional Commit history. The release pull request selects the next version,
updates `CHANGELOG.md`, and records the version in
`.release-please-manifest.json`; merging ordinary changes does not publish a
release. While the module is below v1, a breaking change advances the minor
version rather than declaring the API stable.

Commit and squash-merge subjects use Conventional Commit prefixes. `fix:`
selects a patch, `feat:` selects a minor version, and a `!` or
`BREAKING CHANGE:` footer marks an incompatible change. Maintainers review the
proposed version and release notes before merging the release pull request.

The preparation action uses the built-in `GITHUB_TOKEN` by default. Configure a
fine-grained `RELEASE_PLEASE_TOKEN` with repository contents and pull-request
write access when CI must run automatically on the generated pull request;
GitHub suppresses workflow events caused by its built-in token.

`release.yml` remains manually initiated with `workflow_dispatch`, but no longer
accepts a version input. It reads the reviewed version from the manifest and
runs from the default branch in a protected `release` environment requiring
maintainer approval. Only the release job receives:

```yaml
permissions:
  contents: write
```

The publication workflow:

1. resolves the exact default-branch commit to release;
2. rejects an unprepared, existing, malformed, or incompatible version/tag;
3. runs module-policy, compatibility, full-test, race, coverage, documentation,
   and security gates;
4. verifies the prepared changelog section and uses it as the release notes;
5. creates the immutable `vX.Y.Z` tag and GitHub Release;
6. requests the version through `proxy.golang.org`;
7. verifies it from a clean temporary consumer module;
8. records the version, commit SHA, Go versions, and result in the summary.

The proxy discovery check is:

```sh
GOPROXY=https://proxy.golang.org \
  go list -m github.com/ayeshLK/lib-websubhub@v0.1.0
```

Proxy propagation is eventually consistent, so post-publication verification
uses bounded retry. A failure never causes the workflow to replace the tag.

## 6. Repository controls

Protect the default branch with a GitHub ruleset that requires pull requests,
review, CI and security checks, and conversation resolution, and that blocks
force pushes, deletion, and normal maintainer bypass.

Protect tags matching `v*` so only the approved release process can create
them, and block tag updates and deletion. Enable immutable GitHub Releases when
available.

No workflow triggered from an untrusted pull request receives write permission,
release credentials, or an environment secret. A release workflow never
executes unreviewed pull-request code with privileged credentials.

References:

- [Building and testing Go with GitHub Actions](https://docs.github.com/en/actions/tutorials/build-and-test-code/go)
- [Secure use of GitHub Actions](https://docs.github.com/en/actions/reference/security/secure-use)
- [GitHub repository rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets)
- [GitHub immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)

## 7. Release readiness checklist

A release candidate is ready only when:

- the module path is `github.com/ayeshLK/lib-websubhub` and the license is Apache-2.0;
- both supported Go versions pass their required validation;
- statement coverage is at least 85% and the protocol matrix is complete;
- race, fuzz-regression, cancellation, lifecycle, TLS, authentication-boundary,
  and credential-isolation tests pass;
- the module graph contains no external dependency;
- all exported identifiers are documented and examples compile;
- W3C conformance and project-extension behavior matrices are current;
- no unresolved high-severity security finding remains;
- release notes identify compatibility and security implications;
- a clean consumer can resolve and test the candidate version.

Failure before tagging aborts without publishing. Failure after tagging is
repaired with a new version; a published tag is never mutated.
