# Security policy

## Reporting a vulnerability

Do not disclose suspected vulnerabilities in a public issue. Use GitHub's
private vulnerability reporting or Security Advisory interface for
`ayeshLK/lib-websubhub`. Include affected versions, reproduction steps, impact,
and any suggested mitigation. If private reporting is not enabled, contact the
repository owner privately through their GitHub profile and request a secure
reporting channel without including exploit details in the first message.

Please allow time to confirm the report and coordinate a fix before public
disclosure.

## Supported versions

Until the first tagged release, only the current default branch receives
security fixes. After releases begin, this section will identify supported
release lines explicitly.

## Deployment boundary

This package is a protocol layer, not a complete hardened service. Applications
must provide TLS, authentication, authorization, rate limiting, durable state,
callback and topic SSRF controls, trusted proxy handling, operational limits,
and secret-safe logging. See `docs/spec.md` for the full supported/unsupported
security matrix.

The handler decodes percent-encoded URL unreserved characters before invoking
application callbacks. Apply authorization and destination policy to those
normalized values and enforce the same policy at dial time.
