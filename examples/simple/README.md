# Simple Example

This example runs a local broker and uses the Go SDK to publish and consume.

## Start the broker

```bash
go run ./cmd/rabbimq/main.go
```

### YAML config (server + client)

`examples/simple/rabbimq.yaml`:

```yaml
passphrase: secret

limits:
  maxQueueDepth: 1000
  maxChannelDepth: 200
  maxChannels: 20
  maxConsumers: 50
  maxMessageBytes: 1048576

address: http://127.0.0.1:8080
timeout: 10s
insecure_skip_verify: false
ca_cert_file: ""
```

Server-only keys (`limits`) are ignored by clients and client-only keys (`address`, `timeout`) are ignored by the server.

Run with config:

```bash
go run ./cmd/rabbimq/main.go -config ./examples/simple/rabbimq.yaml
```

## Producer (Go SDK)

Create `producer.go`:

```go
package main

import (
	"context"
	"log"
	"time"

	"RabbiMQ/sdk"
)

func main() {
	client, err := sdk.NewClient(sdk.Config{
		Address: "http://127.0.0.1:8080",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Publish(ctx, "orders", []byte("new order")); err != nil {
		log.Fatal(err)
	}
}
```

Run it:

```bash
go run ./producer.go
```

## Consume tasks (streaming) with the SDK

Create `consumer.go`:

```go
package main

import (
	"context"
	"log"
	"time"

	"RabbiMQ/sdk"
)

func main() {
	client, err := sdk.NewClient(sdk.Config{
		Address: "http://127.0.0.1:8080",
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		// One long-lived stream: no task is lost between reads.
		msgs, errs := client.Subscribe(context.Background(), "orders")
		for msg := range msgs {
			log.Printf("received: %s", string(msg))
		}
		log.Printf("stream ended: %v", <-errs)
		time.Sleep(time.Second) // back off before reconnecting
	}
}
```

Run it:

```bash
go run ./consumer.go
```

Run with config:

```bash
go run ./cmd/rabbimq/main.go publish -config ./examples/simple/rabbimq.yaml "new order"
go run ./cmd/rabbimq/main.go consume -config ./examples/simple/rabbimq.yaml
```

## Real-world challah delivery example

Run the challah producer:

```bash
go run -tags example ./examples/simple/challah_producer.go -config ./examples/simple/rabbimq.yaml
```

Run the challah consumer:

```bash
go run -tags example ./examples/simple/challah_consumer.go -config ./examples/simple/rabbimq.yaml
```
