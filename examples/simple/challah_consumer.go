//go:build example

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"RabbiMQ/sdk"
)

func main() {
	configPath := flag.String("config", "./examples/simple/rabbimq.yaml", "Path to config file")
	flag.Parse()

	client, err := sdk.NewClientFromFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	backoff := time.Second
	for ctx.Err() == nil {
		// Subscribe keeps one stream open, so no task is lost between reads.
		msgs, errs := client.Subscribe(ctx, "orders")
		for msg := range msgs {
			backoff = time.Second
			log.Printf("Customer received: %s", string(msg))
		}
		if err := <-errs; err != nil {
			log.Printf("stream ended: %v (reconnecting in %s)", err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}
