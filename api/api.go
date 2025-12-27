package api

import (
	"RabbiMQ/broker"
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
)

// RabbiMQ is the interface for the RabbiMQ message broker.
type RabbiMQ interface {
	// Publish adds a task to the broker.
	Publish(task []byte) error
	// Consume returns a channel that receives tasks.
	Consume() <-chan []byte
	// Shutdown gracefully shuts down the broker.
	Shutdown()
}

// rabbimq is a concrete implementation of the RabbiMQ interface.
type rabbimq struct {
	broker *broker.Broker
}

// New creates a new RabbiMQ instance.
func New() RabbiMQ {
	clock := &shabbat.RealClock{}
	shabbatClock := shabbat.NewShabbatClock(clock)
	q := queue.NewInMemoryQueue()
	b := broker.NewBroker(shabbatClock, q)
	return &rabbimq{broker: b}
}

// Publish adds a task to the broker.
func (r *rabbimq) Publish(task []byte) error {
	return r.broker.Publish(task)
}

// Consume returns a channel that receives tasks.
func (r *rabbimq) Consume() <-chan []byte {
	return r.broker.Consume()
}

// Shutdown gracefully shuts down the broker.
func (r *rabbimq) Shutdown() {
	r.broker.Shutdown()
}
