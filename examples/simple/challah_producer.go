//go:build example

package main

import (
	"context"
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
		if err := client.Publish(ctx, "orders", payload); err != nil {
			cancel()
			log.Printf("publish failed: %v", err)
			continue
		}
		cancel()

		log.Println("Order sent:", string(payload))
	}
}
