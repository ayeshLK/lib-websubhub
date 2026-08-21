# Changelog

## 0.5.0 (2026-08-21)


### Features

* add event-driven Kafka hub example ([37776c4](https://github.com/ayeshLK/lib-websubhub/commit/37776c494075b523864499a279536fd23f048572))
* add in-memory hub example ([76a7fb0](https://github.com/ayeshLK/lib-websubhub/commit/76a7fb0f05bd4d6ccb8e44cd3431849b50830ca1))
* add Kafka state consolidator ([23818bb](https://github.com/ayeshLK/lib-websubhub/commit/23818bb198af07a939353ee291d31e0e622f57fd))
* add Kafka-backed hub example ([e27746f](https://github.com/ayeshLK/lib-websubhub/commit/e27746f5f27e54e9e012d1da37437cb84d8aa076))
* add WebSub hub framework ([86641fa](https://github.com/ayeshLK/lib-websubhub/commit/86641fac3720afce5d40dd8e986069e88e3d03a4))


### Bug Fixes

* accept Release Please's generated changelog heading during publication
* complete empty Kafka event replay ([9fec070](https://github.com/ayeshLK/lib-websubhub/commit/9fec0705603c0596d58aa13187ba5aad0c401101))
* keep initial release pre-v1 ([bdb75b4](https://github.com/ayeshLK/lib-websubhub/commit/bdb75b4135936bee1f465b74b45d2fd481d96589))
* normalize WebSub URL percent encoding ([434593d](https://github.com/ayeshLK/lib-websubhub/commit/434593d66c55cc6007679d18f4d0afe448502296))
* normalize WebSub URL percent encoding ([cf89cb5](https://github.com/ayeshLK/lib-websubhub/commit/cf89cb539d84eb67435db96bc9921a9f97c1349d))

## Changelog

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
