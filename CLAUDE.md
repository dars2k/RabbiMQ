# RabbiMQ — Claude Development Guide

## Project Overview

**RabbiMQ** is a Shabbat-aware message broker written in Go. Its core feature is automatically pausing all message publishing and delivery during Shabbat (Friday 5 PM – Saturday 8 PM local time) and resuming afterward.

> "Six days you shall process messages, but on the seventh, you shall block."

## Repository Structure

```
RabbiMQ/
├── cmd/rabbimq/          # CLI entry point (server, publish, consume commands)
├── api/                  # High-level RabbiMQ facade (wraps broker + HTTP server)
├── broker/               # Core message broker: multi-channel, Shabbat-aware
├── queue/                # Queue interface + in-memory and persistent implementations
├── shabbat/              # Shabbat time logic and clock abstraction
├── http/                 # HTTP API server (publish, consume, metrics endpoints)
├── sdk/                  # Go client SDK for remote access
├── config/               # YAML/JSON configuration file loader
└── examples/             # Usage examples (simple SDK, Docker)
```

## Build & Run

```bash
# Install CLI binary
go install ./cmd/rabbimq

# Run server (default: HTTP on 127.0.0.1:8080)
go run ./cmd/rabbimq/main.go

# Run with TLS
go run ./cmd/rabbimq/main.go -addr 127.0.0.1:8443 -tls-cert ./server.crt -tls-key ./server.key

# Run with config file
go run ./cmd/rabbimq/main.go -config ./examples/simple/rabbimq.yaml
```

## Testing

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./broker/...
go test ./queue/...
```

## Docker

```bash
docker build -t rabbimq .
docker run --rm -p 8080:8080 rabbimq

# Or with Docker Compose
docker compose up --build
```

## CLI Usage

```bash
# Publish a message (default channel)
go run ./cmd/rabbimq/main.go publish "my task"

# Publish to a named channel
go run ./cmd/rabbimq/main.go publish -channel orders "new order"

# Consume (streams via SSE, blocks until cancelled)
go run ./cmd/rabbimq/main.go consume

# Consume one message and exit
go run ./cmd/rabbimq/main.go consume -channel orders -once
```

## HTTP API

| Method | Endpoint     | Description                                      |
|--------|--------------|--------------------------------------------------|
| POST   | `/publish`   | Publish task body; `?channel=name` query param   |
| GET    | `/consume`   | Server-Sent Events stream; `?channel=name`       |
| GET    | `/metrics`   | JSON metrics (counts, queue depth, Shabbat state)|
| GET    | `/channels`  | Per-channel queue depth map                      |

Authentication: optional `X-RabbiMQ-Key` header matching the configured passphrase.

## Configuration (YAML/JSON)

```yaml
passphrase: secret
address: http://127.0.0.1:8080
timeout: 10s
insecure_skip_verify: false
ca_cert_file: ""
limits:
  maxQueueDepth: 1000      # total messages across all channels
  maxChannelDepth: 200     # messages per channel
  maxChannels: 20
  maxConsumers: 50
  maxMessageBytes: 1048576 # 1 MB
```

## Architecture Notes

### Shabbat Logic (`shabbat/`)
- Shabbat window: Friday 17:00 → Saturday 20:00 (local time)
- Uses a `Clock` interface (`RealClock` / `FixedClock`) for deterministic testing
- No external time APIs — local system time only

### Broker (`broker/`)
- Each channel gets its own queue and a dedicated delivery goroutine
- `Publish` returns `ErrShabbatObserved` during Shabbat
- Delivery goroutines block on a notification channel and resume when Shabbat ends
- Limits enforced: `MaxQueueDepth`, `MaxChannelDepth`, `MaxChannels`, `MaxConsumers`, `MaxMessageBytes`

### Queue (`queue/`)
- Common `Queue` interface: `Enqueue`, `Dequeue`, `Size`, `Close`
- **In-memory**: thread-safe slice-based FIFO
- **Persistent**: disk-backed binary format (`[4-byte length][data]`); compacted on startup

### SDK (`sdk/`)
- Typed Go client wrapping the HTTP API
- Supports config files, TLS (CA cert, insecure mode), and context-based cancellation
- SSE consumer via `stream.go` with 15-second keepalive pings

## Key Patterns

- **Dependency injection** for the clock (testability)
- **Interface-based** queue abstraction (swap in-memory ↔ persistent)
- **Goroutine-per-channel** delivery manager
- **`sync/atomic`** for lock-free metric counters
- **Graceful shutdown** coordinated via `sync.WaitGroup`

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`):
- **CI**: `go test ./...` on every push and PR
- **Release** (on `v*` tags): cross-platform binaries for Linux, macOS (amd64/arm64), and Windows uploaded to GitHub Releases
