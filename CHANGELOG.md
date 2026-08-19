# Changelog

All notable changes to this project will be documented here. The project uses
[Semantic Versioning](https://semver.org/).

## Unreleased

### Changed

- Automate semantic-version selection and changelog preparation through a
  reviewable release pull request.
- Run release preparation only when a maintainer requests it.

### Fixed

- Document the repository permission required for Release Please to create its
  pull request with the built-in GitHub Actions token.
- Prepare v0.5.0 as the first automated release instead of accepting Release
  Please's default initial version of v1.0.0.

### Removed

- Remove the non-standard delivery message ID field and header from the
  framework API and wire behavior.

### Added

- Kafka-backed hub example that writes concrete framework state values to an
  eventually consistent `websub-events` projection, with durable update
  consumption, bounded delivery retries, and dead-letter handling.
- Initial standard-library-only WebSub hub framework.
- In-memory WebSub hub example.
- Architecture, testing, documentation, and release plans.
- Optional publisher extension using the `X-Go-Publisher` mode header.
