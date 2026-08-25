# In-memory WebSub hub

This directory contains a runnable hub built with the Go `websubhub` library and
only the Go standard library.

The sample deliberately keeps hub policy in application code:

- registered topics and verified subscriptions are held in memory;
- subscription renewal replaces the existing callback/topic entry;
- the application calculates and enforces lease expiry;
- content publications are queued and delivered by a bounded worker pool;
- event-only notifications cause the hub to fetch the registered topic before
  distributing it;
- a subscriber response of HTTP `410 Gone` removes that subscription; and
- deregistering a topic also removes its subscriptions.

It has no authentication, authorization, TLS termination, or durable storage.
It is intended for local evaluation, not production deployment.

## Requirements

- Go 1.25 or later
- the repository's root module

The sample is a separate Go module. Its `go.mod` uses a local `replace` directive
so it always tests the library implementation at `../..`.

## Build and run

From this directory:

```sh
go test ./...
go build -o in-memory-websubhub .
./in-memory-websubhub
```

Or run it directly:

```sh
go run .
```

The default endpoint is `http://localhost:9090/hub`. Stop it with `Ctrl+C`.
Available flags are:

```text
-addr string
      HTTP listen address (default ":9090")
-delivery-workers int
      number of concurrent delivery workers (default 4)
-hub-url string
      public absolute hub URL used in verification and delivery
      (default "http://localhost:9090/hub")
-path string
      hub HTTP path (default "/hub")
```

`-hub-url` is the URL visible to callbacks and must have the same path as
`-path`. For example:

```sh
go run . -addr :8080 -path /websub -hub-url http://localhost:8080/websub
```

## Manual protocol walk-through

The commands below use this topic URL:

```text
http://localhost:8081/topics/orders
```

WebSub topics must be absolute HTTP(S) resource URLs. For an event-only
notification, the URL must also be reachable because the sample fetches its
current representation with `GET`.

### 1. Register a topic

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=register' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders' \
  --data-urlencode 'hub.content_type=application/json; charset=utf-8'
```

A successful registration response is HTTP `200 OK` with an `accepted` form
response.
The optional content type is retained as application-owned topic metadata. The
media type supplied with an actual publication or topic fetch remains
authoritative for delivery.

### 2. Subscribe a callback

Start a subscriber callback that implements WebSub intent verification: on a
`GET`, it must return the exact `hub.challenge` value with a successful status.
It must accept content distribution with `POST`. Then submit:

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=subscribe' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders' \
  --data-urlencode 'hub.callback=http://localhost:8082/callback' \
  --data-urlencode 'hub.lease_seconds=300' \
  --data-urlencode 'hub.secret=local-test-secret'
```

The initial response is HTTP `202 Accepted`. Verification and persistence happen
asynchronously. Repeating the request renews the subscription after successful
verification.

### 3. Publish content

Use the optional `PublisherClient` from a publisher application:

```go
publisher, err := websubhub.NewPublisherClient(websubhub.PublisherClientConfig{
    HubURL: "http://localhost:9090/hub",
})
if err != nil {
    log.Fatal(err)
}

err = publisher.Publish(context.Background(), websubhub.UpdateMessage{
    Topic:       "http://localhost:8081/topics/orders",
    ContentType: "application/json; charset=utf-8",
    Body:        []byte(`{"order":"A-42","state":"created"}`),
})
if err != nil {
    log.Fatal(err)
}
```

The hub queues the update and returns HTTP `202 Accepted`. The callback receives
the same payload and exact `Content-Type`, WebSub `Link` headers, and an
`X-Hub-Signature` when the subscription supplied a secret.

To send an event-only notification instead:

```go
err = publisher.Notify(
    context.Background(),
    "http://localhost:8081/topics/orders",
)
if err != nil {
    log.Fatal(err)
}
```

The dispatcher performs `GET http://localhost:8081/topics/orders` and distributes
the returned body and `Content-Type`.

### 4. Unsubscribe

The callback must again echo the verification challenge:

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=unsubscribe' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders' \
  --data-urlencode 'hub.callback=http://localhost:8082/callback'
```

The verified callback/topic entry is then removed.

### 5. Deregister the topic

```sh
curl -i -X POST http://localhost:9090/hub \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'hub.mode=deregister' \
  --data-urlencode 'hub.topic=http://localhost:8081/topics/orders'
```

Because all state is in memory, restarting the process removes every topic and
subscription.
