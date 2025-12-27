//go:build example

package main

import (
	"context"
	"flag"
	"log"

	"RabbiMQ/sdk"
)

func main() {
	configPath := flag.String("config", "./examples/simple/rabbimq.yaml", "Path to config file")
	flag.Parse()

	client, err := sdk.NewClientFromFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, ok, err := client.Consume(context.Background(), "orders")
		if err != nil {
			log.Printf("consume error: %v", err)
			continue
		}
		if ok {
			log.Printf("Customer received: %s", string(msg))
		}
	}
}
