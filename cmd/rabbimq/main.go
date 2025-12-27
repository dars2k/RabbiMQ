package main

import (
	"RabbiMQ/api"
	"fmt"
	"log"
	"time"
)

func main() {
	log.Println("Starting RabbiMQ...")

	// Create a new RabbiMQ instance.
	mq := api.New()
	defer mq.Shutdown()

	// Start a consumer in a separate goroutine.
	go func() {
		consumer := mq.Consume()
		for task := range consumer {
			log.Printf("Consumer received task: %s", string(task))
		}
	}()

	// Publish some messages.
	log.Println("Publishing messages...")
	for i := 0; i < 5; i++ {
		task := []byte(fmt.Sprintf("task-%d", i))
		err := mq.Publish(task)
		if err != nil {
			log.Printf("Failed to publish task: %v", err)
		} else {
			log.Printf("Published task: %s", string(task))
		}
	}

	// Wait for a moment to allow the consumer to process the tasks.
	time.Sleep(2 * time.Second)

	log.Println("RabbiMQ shut down.")
}