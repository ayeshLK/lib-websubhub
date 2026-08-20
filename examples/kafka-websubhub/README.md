# Kafka-backed WebSub hub

This directory contains a runnable Go hub that integrates `lib-websubhub` with
Apache Kafka through `franz-go`. It is an independent module, so Kafka and its
transitive dependencies do not enter the core module graph.

The example demonstrates an application-owned broker architecture:

- a single-partition `websub-events` log records concrete topic registrations,
  verified unsubscriptions, and deregistrations directly; subscription records
  remain flat but add application-owned `server_id` and optional `status`
  fields;
- validated state changes complete when Kafka acknowledges their event;
- a consolidator persists the current materialized view to the compact
  `websub-events-snapshots` topic and exposes it over HTTP;
- snapshots contain application state only and do not persist Kafka offsets;
- the consolidator and hub replay retained state events through a captured log
  end before their HTTP endpoints start, then tail new events with a
  process-local Kafka cursor;
- only event application changes in-memory state, so dependent requests observe
  eventual consistency;
- every WebSub topic maps to
  `websub-topic-<sha256(websub-topic-url)>`, which stores JSON updates;
- each subscription is owned by the `server_id` of the hub that accepted it;
  every hub caches it, but only the owning instance starts its Kafka consumer;
- every owned subscription uses a consumer group derived from the topic,
  callback, and original `LeaseStartedAt`;
- a new group starts at the end of its content topic and commits a polled batch
  only after every record is delivered successfully; and
- exhausted delivery retries publish a flat subscription record with
  `status: "stale"` to `websub-events`, leaving the content offset
  uncommitted.

Validations read the current in-memory projection. A dependent request can
therefore be denied until its prerequisite event is consumed, and equivalent
requests can both publish events while the projection is catching up. Event
application is idempotent, and clients should retry dependent operations after
the projection advances.
Kafka delivery is at least once. A crash after HTTP success but before the
batch commit can redeliver records to that subscriber. Subscribers must
tolerate duplicates using application-level semantics; the hub adds no
non-standard WebSub delivery identifier. An unsubscribe followed by a new
subscription receives a new consumer group and does not inherit the earlier
group's backlog. Resubscribing a stale subscription reuses its original group, server
owner, and uncommitted offset.

## Scope and security

This is a local architecture example, not a production hub or a W3C conformance
claim. It supports multiple hub instances with unique, stable server IDs and
one consolidator, but does not provide automatic subscription failover or
rebalancing. It also omits inbound authentication, TLS, rate limiting, metrics,
and callback/topic SSRF controls.
The supplied Kafka listener is plaintext. The example also assumes JSON
content and intentionally omits lease-expiry scheduling; a conforming
production hub must expire subscriptions and continue appropriate delivery
attempts for active subscriptions.

Subscription secrets are application state and are stored in the Kafka event
log so restarted delivery workers can calculate WebSub signatures. A production
deployment must protect that topic with TLS, authentication, least-privilege
ACLs, encryption and retention policy, and restricted operator access. Use a
dial-time network policy for subscriber callbacks and event-only topic fetches.
Multiple hub writers additionally need an atomic admission/state design rather
than relying on the single-process checks shown here. One Kafka consumer and
consumer group per subscription is intentionally illustrative and should be
capacity-tested before production use.

## Requirements

- Go 1.25 or later
- Docker with Compose, or an existing Kafka cluster

The module uses a local `replace` directive for the library at `../..`.

Each process reads `Config.toml` from its working directory:

- `cmd/websubhub/Config.toml` contains the `[websubhub]` settings. Assign a
  unique, stable `server_id` to every deployed hub instance.
- `cmd/consolidator/Config.toml` contains the `[consolidator]` settings.

Unknown keys, invalid durations, and missing required values cause startup to
fail.

## Start Kafka and the hub

From this directory, start Kafka:

```sh
docker compose up -d
```

Compose starts Kafka in single-node KRaft mode, creates the single-partition
`websub-events` log and compact `websub-events-snapshots` topic, and permits
automatic creation of one-partition content topics.

Start the consolidator and hub from their respective directories in separate
terminals:

```sh
cd cmd/consolidator
go run .
```

```sh
cd cmd/websubhub
go run .
```

The consolidator exposes `http://localhost:9091/state-snapshot` and
`http://localhost:9091/healthz`. The hub listens at
`http://localhost:9090/hub`, with health at `http://localhost:9090/healthz`.

For an existing cluster, create the single-partition event topic and permit
automatic content-topic creation for this example. Set `brokers` in both
configuration files, for example:

```toml
brokers = ["broker-1:9092", "broker-2:9092"]
```

Start the consolidator before the hub. Stop both processes with `Ctrl+C`;
stop Kafka with `docker compose down`.

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

The hub accepts only valid JSON content and acknowledges only after Kafka
accepts the update on the mapped topic. Each subscription worker independently
delivers and commits that content. Event-only notification, unsubscription, and
deregistration use the same requests documented by the in-memory example.

## Tests

Tests use an in-process broker implementation, so they require no running Kafka
while exercising replay, asynchronous state projection, topic/group mapping,
per-subscription workers, JSON materialization, retry, stale state, batch
commit, signatures, and delivery behavior:

```sh
gofmt -w cmd consolidator internal websubhub
go vet ./...
go test -race ./...
```

The real Kafka adapter is compiled by those commands and exercised when running
the example against Compose or another Kafka cluster.
