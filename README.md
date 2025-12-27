# RabbiMQ

RabbiMQ is a Shabbat-aware message broker written in Go. It queues work normally during the week and pauses all publishing and delivery during Shabbat, then resumes automatically.

## Table of Contents

- [Features](#features)
- [How Shabbat Mode Works](#how-shabbat-mode-works)
- [Installation](#installation)
- [Usage](#usage)
- [Network API](#network-api)
- [Go SDK](#go-sdk)
- [Examples](#examples)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Features

- Shabbat-aware queuing (no publish or delivery during Shabbat)
- HTTP API with Server-Sent Events for streaming consumption
- Per-channel queues and metrics
- Optional TLS support
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

When connecting to a server with a self-signed certificate:

```bash
go run ./cmd/rabbimq/main.go publish -addr https://127.0.0.1:8443 -insecure "my new task"
```

Run with a shared server/client config file:

```bash
go run ./cmd/rabbimq/main.go -config ./examples/simple/rabbimq.yaml
```

Docker build and run:

```bash
docker build -t rabbimq .
docker run --rm -p 8080:8080 rabbimq
```

Docker Compose:

```bash
docker compose up --build
```

## Network API

RabbiMQ provides a simple HTTP-based API for remote clients.

### Publish a Task

- **Endpoint:** `/publish`
- **Method:** `POST`
- **Body:** The raw task data
- **Optional query:** `?channel=<name>`

```bash
curl -X POST --data "my new task" http://127.0.0.1:8080/publish
```

### Consume Tasks (SSE)

- **Endpoint:** `/consume`
- **Method:** `GET`
- **Optional query:** `?channel=<name>`

This endpoint streams tasks using Server-Sent Events.

```bash
curl -N http://127.0.0.1:8080/consume
```

### Metrics

- **Endpoint:** `/metrics`
- **Method:** `GET`

Returns JSON metrics, including tasks published, delivered, rejected, and the number held during Shabbat.

### Channels

- **Endpoint:** `/channels`
- **Method:** `GET`

Returns a JSON array of channels with their pending queue depth.

```bash
curl http://127.0.0.1:8080/channels
```

## Go SDK

The SDK lets you publish and consume tasks without manually building HTTP requests. It uses a simple JSON config file.

```json
{
  "address": "http://127.0.0.1:8080",
  "timeout": "10s",
  "insecure_skip_verify": false,
  "ca_cert_file": "",
  "channel": "orders"
}
```

See `examples/simple/README.md` for end-to-end SDK usage.

## Examples

- Simple SDK example: `examples/simple/README.md`
- Docker setup: `examples/docker/docker-compose.yml`

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
