# Kafka-backed WebSub hub

This directory contains a runnable Go hub that integrates `lib-websubhub` with
Apache Kafka through `franz-go`. It is an independent module, so Kafka and its
transitive dependencies do not enter the core module graph.

The example demonstrates an application-owned broker architecture:

- a single-partition `websub-events` log records topic registrations,
  verified subscriptions, verified unsubscriptions, and deregistrations;
- validated state changes complete when Kafka acknowledges their event;
- only the event replay/tail worker applies those events to in-memory state, so
  dependent requests observe eventual consistency;
- every process restart replays that log before the HTTP endpoint starts;
- `websubhub-updates` durably buffers exact content before delivery;
- the `websubhub-delivery` consumer group commits an update only after fan-out
  succeeds or bounded retries write it to `websubhub-dead-letter`; and
- Kafka topic/partition/offset supplies an internal dead-letter record reference.

Validations read the current in-memory projection. A dependent request can
therefore be denied until its prerequisite event is consumed, and equivalent
requests can both publish events while the projection is catching up. Event
application is idempotent, and clients should retry dependent operations after
the projection advances.
Kafka delivery is at least once. If one callback fails, retrying the update can
send duplicates to callbacks that already succeeded. Subscribers must tolerate
duplicate content using application-level semantics; the hub adds no
non-standard WebSub delivery identifier.

## Scope and security

This is a local architecture example, not a production hub or a W3C conformance
claim. It intentionally runs one hub instance and does not provide inbound
authentication, TLS, rate limiting, metrics, or callback/topic SSRF controls.
The supplied Kafka listener is plaintext.

Subscription secrets are application state and are stored in the Kafka event
log so restarted delivery workers can calculate WebSub signatures. A production
deployment must protect that topic with TLS, authentication, least-privilege
ACLs, encryption and retention policy, and restricted operator access. Use a
dial-time network policy for subscriber callbacks and event-only topic fetches.
Multiple hub writers additionally need an atomic admission/state design rather
than relying on the single-process checks shown here.

## Requirements

- Go 1.25 or later
- Docker with Compose, or an existing Kafka cluster

The module uses a local `replace` directive for the library at `../..`.

## Start Kafka and the hub

From this directory:

```sh
docker compose up -d
go run .
```

Compose starts Kafka in single-node KRaft mode and creates the three required
single-partition topics. The hub listens at `http://localhost:9090/hub`; its
health endpoint is `http://localhost:9090/healthz`.

For an existing cluster, create the event, update, and dead-letter topics with
one partition each, then run:

```sh
go run . -brokers broker-1:9092,broker-2:9092
```

Useful options include:

```text
-delivery-attempts int
      bounded delivery attempts before dead-lettering (default 3)
-delivery-group string
      Kafka consumer group used by delivery workers (default "websubhub-delivery")
-events-topic string
      state-event topic (default "websub-events")
-retry-backoff duration
      delay between delivery attempts (default 1s)
-startup-timeout duration
      Kafka connection and event replay timeout (default 30s)
```

Run `go run . -help` for topic names and HTTP options. Stop the hub with
`Ctrl+C`; stop Kafka with `docker compose down`.

## Protocol walkthrough

Register an absolute HTTP(S) topic:

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=register' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders'
```

This response confirms Kafka accepted the event, not that the in-memory view is
already updated. Before issuing a dependent request in this walkthrough, wait
for the `state event applied` log entry for that topic.

Start a subscriber callback that echoes `hub.challenge` for verification GETs
and accepts delivery POSTs. Then subscribe:

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=subscribe' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders' \
  --data-urlencode 'hub.callback=http://localhost:8082/callback' \
  --data-urlencode 'hub.lease_seconds=300' \
  --data-urlencode 'hub.secret=local-test-secret'
```

Publish exact content through the optional publisher extension:

```sh
curl -i -X POST \
  'http://localhost:9090/hub?hub.mode=publish&hub.topic=http%3A%2F%2Flocalhost%3A8081%2Ftopics%2Forders' \
  -H 'X-Go-Publisher: publish' \
  -H 'Content-Type: application/json; charset=utf-8' \
  --data-binary '{"order":"A-42","state":"created"}'
```

The hub acknowledges only after Kafka accepts the update. The delivery worker
then sends it to subscriptions currently visible in the in-memory projection.
Event-only notification, unsubscription,
and deregistration use the same requests documented by the in-memory example.

## Tests

Tests use an in-process broker implementation, so they require no running Kafka
while exercising replay, asynchronous state projection, materialization, retry,
dead-letter, signature, and delivery behavior:

```sh
gofmt -w *.go
go vet ./...
go test -race ./...
```

The real Kafka adapter is compiled by those commands and exercised when running
the example against Compose or another Kafka cluster.
