# Changelog

All notable changes to this project will be documented here. The project uses
[Semantic Versioning](https://semver.org/).

## Unreleased

### Changed

- Consolidate testing and release guidance into `CONTRIBUTING.md` and
  `AGENTS.md`, leaving `docs/spec.md` as the durable framework document.
- Move framework improvement tracking out of the normative specification while
  preserving its current API and conformance boundaries.
- Rewrite `docs/spec.md` as a maintainer-facing, main-branch specification with
  normative Go architecture, validation, execution, lifecycle, and security rules.
- Automate semantic-version selection and changelog preparation through a
  reviewable release pull request.
- Run release preparation only when a maintainer requests it.
- Keep Kafka replay offsets process-local instead of persisting a next offset
  in consolidated application snapshots.
- Load revisioned Kafka state snapshots across topic partitions using a
  partition-agnostic polling loop.
- Configure the Kafka hub and consolidator through separate, typed
  `Config.toml` files instead of command-line flags.
- Persist a stable hub `server_id` with each subscription and start delivery
  workers only on the owning hub instance.
- Improve repository discovery, README onboarding, and Go API documentation.

### Fixed

- Complete Kafka state replay immediately when `websub-events` is empty
  instead of waiting for the startup context to expire.
- Document the repository permission required for Release Please to create its
  pull request with the built-in GitHub Actions token.
- Prepare v0.5.0 as the first automated release instead of accepting Release
  Please's default initial version of v1.0.0.
- Decode percent-encoded unreserved characters in inbound topic and callback
  URLs so equivalent WebSub subscription identities reach application callbacks
  and verification consistently.

### Removed

- Remove the non-standard delivery message ID field and header from the
  framework API and wire behavior.

### Added

- Kafka-backed hub example with an eventually consistent `websub-events`
  projection, hashed content topics, one consumer group per subscription,
  JSON delivery, bounded retries, and application-owned stale-subscription
  state.
- Initial standard-library-only WebSub hub framework.
- In-memory WebSub hub example.
- Architecture, testing, documentation, and release plans.
- Optional publisher extension using the `X-Go-Publisher` mode header.
- Codecov reporting with an absolute 85% coverage floor, a zero-regression
  project check, an 85% changed-line check, and a public README badge.
