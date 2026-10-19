//go:build example

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
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

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	addresses := []string{
		"23 Herzl St, Apt 4",
		"7 Ben Yehuda St, Suite 2",
		"15 Allenby Rd",
		"42 King George St",
	}

	for {
		sleepFor := time.Duration(rng.Intn(10)+1) * time.Second
		time.Sleep(sleepFor)

		orderID := rng.Intn(100000)
		address := addresses[rng.Intn(len(addresses))]
		payload := []byte(fmt.Sprintf("Deliver challah to %s (order #%d)", address, orderID))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := client.Publish(ctx, "orders", payload)
		cancel()
		if sdk.IsShabbat(err) {
			// The bakery rests too: wait until the broker accepts orders again.
			var apiErr *sdk.APIError
			if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
				log.Printf("Shabbat Shalom! Resuming orders in %s", apiErr.RetryAfter)
				time.Sleep(apiErr.RetryAfter)
			}
			continue
		}
		if err != nil {
			log.Printf("publish failed: %v", err)
			continue
		}

		log.Println("Order sent:", string(payload))
	}
}
