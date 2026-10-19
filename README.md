# RabbiMQ

RabbiMQ is a Shabbat-aware message broker written in Go. It queues work normally during the week and pauses all publishing and delivery during Shabbat, then resumes automatically.

## Table of Contents

- [Features](#features)
- [How Shabbat Mode Works](#how-shabbat-mode-works)
- [Installation](#installation)
- [Usage](#usage)
- [Network API](#network-api)
- [Go SDK](#go-sdk)
- [Configuration](#configuration)
- [Persistence and Delivery Guarantees](#persistence-and-delivery-guarantees)
- [Examples](#examples)
- [Development](#development)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Features

- Shabbat-aware queuing (no publish or delivery during Shabbat)
- HTTP API with Server-Sent Events for streaming consumption
- Per-channel FIFO queues, limits and metrics
- Durable, self-compacting on-disk queues that survive restarts
- Optional TLS and passphrase authentication
- Go SDK and CLI (`publish`, `consume`, `metrics`, `channels`)
- Graceful shutdown on `SIGINT`/`SIGTERM`
- Docker and Docker Compose ready

## How Shabbat Mode Works

RabbiMQ enforces Halachic time rules:
- **During Shabbat (Sabbath)**, the broker:
  - ❌ Does not accept new tasks
  - ❌ Does not deliver queued tasks
  - ✅ Safely holds all tasks
- **After Shabbat ends**, the broker:
  - ✅ Automatically resumes
  - ✅ Accepts new tasks
  - ✅ Delivers pending tasks in order

> “Six days you shall process messages, but on the seventh, you shall block.”

Shabbat is determined by the server’s local date and time. No external APIs are used.

- **Shabbat starts:** Friday at 5:00 PM (17:00)
- **Shabbat ends:** Saturday at 8:00 PM (20:00)

This logic is centralized in the `shabbat.ShabbatClock` component. The time is injectable, which allows for deterministic testing.

Publishing is blocked from the exact moment Shabbat begins. A task that is still waiting for a consumer when Shabbat starts is held until Shabbat ends. Publish requests rejected during Shabbat get `503 Service Unavailable` with a `Retry-After` header giving the number of seconds until Shabbat ends.

## Installation

Prerequisites:
- Go 1.25.5+ (see `go.mod`)
- Docker (optional, for containerized runs)

Install the CLI locally:

```bash
go install ./cmd/rabbimq
```

## Usage

Run the broker (HTTP on `127.0.0.1:8080`):

```bash
go run ./cmd/rabbimq/main.go
```

The queue log is written to `rabbimq.log` in the current directory by default. Use `-data` to choose another location:

```bash
go run ./cmd/rabbimq/main.go -data /var/lib/rabbimq/rabbimq.log
```

Run with TLS:

```bash
go run ./cmd/rabbimq/main.go -addr 127.0.0.1:8443 -tls-cert ./server.crt -tls-key ./server.key
```

Publish and consume via CLI:

```bash
go run ./cmd/rabbimq/main.go publish "my new task"
go run ./cmd/rabbimq/main.go consume
```

Channel-specific usage:

```bash
go run ./cmd/rabbimq/main.go publish -channel orders "new order"
go run ./cmd/rabbimq/main.go consume -channel orders
```

The `consume` command streams events by default. Use `-once` to exit after the first message.

Inspect the broker:

```bash
go run ./cmd/rabbimq/main.go metrics
go run ./cmd/rabbimq/main.go channels
```

Client commands accept `-addr`, `-passphrase`, `-config`, `-timeout`, `-ca-cert` and `-insecure`. Explicit flags override values from `-config`. Run `rabbimq <command> -h` for details.

When connecting to a server with a self-signed certificate:

```bash
go run ./cmd/rabbimq/main.go publish -addr https://127.0.0.1:8443 -insecure "my new task"
```

Run with a shared server/client config file:

```bash
go run ./cmd/rabbimq/main.go -config ./examples/simple/rabbimq.yaml
```

Stop the broker with `Ctrl+C` or `SIGTERM`. It stops accepting requests, ends open consume streams and closes its queue files.

Docker build and run:

```bash
docker build -t rabbimq .
docker run --rm -p 8080:8080 -v rabbimq-data:/data rabbimq
```

The image listens on `0.0.0.0:8080`, runs as a non-root user, stores queues in `/data` and has a health check on `/healthz`.

Docker Compose:

```bash
docker compose up --build
```

## Network API

RabbiMQ provides a simple HTTP-based API for remote clients.

When a passphrase is configured, every endpoint except `/healthz` requires the `X-RabbiMQ-Key` header.

Channel names may contain up to 64 letters, digits, `-` and `_`. An empty channel name selects the `default` channel.

### Publish a Task

- **Endpoint:** `/publish`
- **Method:** `POST`
- **Body:** The raw task data
- **Optional query:** `?channel=<name>`

```bash
curl -X POST --data "my new task" http://127.0.0.1:8080/publish
```

Returns `200 OK` on success. Errors carry a plain-text message and a machine-readable code in the `X-RabbiMQ-Error` header:

| Status | `X-RabbiMQ-Error` | Meaning |
| --- | --- | --- |
| 400 | `invalid_channel` | Channel name is not allowed |
| 401 | `unauthorized` | Missing or wrong `X-RabbiMQ-Key` |
| 413 | `message_too_large` | Body exceeds `maxMessageBytes` |
| 429 | `queue_full`, `channel_full`, `channel_limit`, `consumer_limit` | A configured limit was reached |
| 503 | `shabbat` | It is Shabbat; see `Retry-After` |
| 503 | `unavailable` | The broker is shutting down |

### Consume Tasks (SSE)

- **Endpoint:** `/consume`
- **Method:** `GET`
- **Optional query:** `?channel=<name>`, `?once=true`

This endpoint streams tasks using Server-Sent Events. Each task is one event. A task containing newlines is sent as several `data:` lines, which SSE clients join back with `\n`. Idle streams receive a `: ping` comment every 15 seconds.

With `once=true` the server sends a single task and closes the stream, so no other task is taken off the queue for that client.

Consumers of the same channel compete: each task is delivered to exactly one of them, in FIFO order.

```bash
curl -N http://127.0.0.1:8080/consume
```

### Metrics

- **Endpoint:** `/metrics`
- **Method:** `GET`

Returns JSON metrics: tasks `published`, `delivered` and `rejected` (for any reason), `queue_depth`, `held_during_shabbat`, `is_shabbat`, and the current number of `consumers` and `channels`.

### Channels

- **Endpoint:** `/channels`
- **Method:** `GET`

Returns a JSON array of channels with their pending queue depth.

```bash
curl http://127.0.0.1:8080/channels
```

### Health

- **Endpoint:** `/healthz`
- **Method:** `GET`

Returns `200 ok`. It never requires authentication, so it can be used for container health checks.

## Go SDK

The SDK lets you publish and consume tasks without manually building HTTP requests. It can be configured in code or from a JSON or YAML file.

```json
{
  "address": "http://127.0.0.1:8080",
  "timeout": "10s",
  "passphrase": "",
  "insecure_skip_verify": false,
  "ca_cert_file": "",
  "channel": "orders"
}
```

`channel` is the default channel for calls made with an empty channel name.

```go
client, err := sdk.NewClientFromFile("rabbimq.yaml")
if err != nil {
	log.Fatal(err)
}

// Publish, with typed errors.
err = client.Publish(ctx, "orders", []byte("new order"))
var apiErr *sdk.APIError
if sdk.IsShabbat(err) && errors.As(err, &apiErr) {
	log.Printf("Shabbat; retry in %s", apiErr.RetryAfter)
}

// Stream tasks over one connection until ctx is canceled.
msgs, errs := client.Subscribe(ctx, "orders")
for msg := range msgs {
	log.Printf("received %s", msg)
}
if err := <-errs; err != nil {
	log.Printf("stream ended: %v", err) // sdk.ErrStreamClosed if the server went away
}

// Or take exactly one task.
msg, ok, err := client.Consume(ctx, "orders")
```

`Metrics`, `Channels` and `Healthy` wrap the corresponding endpoints. See `examples/simple/README.md` for end-to-end SDK usage.

## Configuration

The server reads a YAML or JSON file passed with `-config`. Command-line flags take precedence.

```yaml
addr: 127.0.0.1:8080        # listen address (-addr)
dataFile: rabbimq.log       # default channel log (-data)
passphrase: secret          # required in X-RabbiMQ-Key when set

limits:                     # 0 means unlimited unless noted
  maxQueueDepth: 1000       # pending tasks across all channels
  maxChannelDepth: 200      # pending tasks per channel
  maxChannels: 20           # channels, including "default"
  maxConsumers: 50          # concurrent /consume streams
  maxMessageBytes: 1048576  # 0 means 1 MiB
```

Negative limits are rejected. The same file can also hold the SDK client settings (see `examples/simple/rabbimq.yaml`).

## Persistence and Delivery Guarantees

- The default channel is stored in the data file (for example `rabbimq.log`), and every other channel in a file next to it (`rabbimq.orders.log`).
- Each log is append-only while the broker runs and is compacted when the broker starts, so consumed tasks are never replayed.
- On startup every channel with a log file is reopened, so its pending tasks are delivered and reported right away.
- A partially written record left by a crash is discarded at startup.
- A task is removed from its queue only after a consumer has received it from the broker. Tasks waiting for a consumer survive shutdowns and restarts.
- Over HTTP, delivery is at-most-once once a task has been handed to a stream: if the client disconnects while the task is in flight, that task is lost. Use `once=true` (the SDK's `Consume`) or a long-lived `Subscribe` stream rather than repeatedly opening and cancelling streams.

## Examples

- Simple SDK example: `examples/simple/README.md`
- Docker setup: `examples/docker/docker-compose.yml`

## Development

```bash
gofmt -l .                     # formatting
go vet ./...                   # static checks
go test -race -cover ./...     # tests with the race detector
```

CI runs all of the above, checks that `go.mod` is tidy, and builds and smoke-tests the Docker image.

## Technologies Used

- Go (HTTP server + CLI)
- Server-Sent Events (streaming consumption)
- Docker (optional packaging)

## Contributing

Contributions are welcome. Please open an issue to discuss changes, then submit a pull request with:
- a clear description of the change
- tests or repro steps when applicable

## License

License information has not been added yet.

## Support

For help or bug reports, open an issue in this repository.
