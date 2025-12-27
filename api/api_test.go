package api

import (
	"RabbiMQ/shabbat"
	"testing"
	"time"
)

func TestPublishConsumeDefaultChannel(t *testing.T) {
	clock := &shabbat.FixedClock{Time: time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)}
	mq := newWithClock(clock, Config{})
	consumer, err := mq.Consume("")
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if err := mq.Publish("", []byte("hello")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-consumer:
		if string(msg) != "hello" {
			t.Fatalf("unexpected message: %q", string(msg))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestPublishConsumeChannel(t *testing.T) {
	clock := &shabbat.FixedClock{Time: time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)}
	mq := newWithClock(clock, Config{})
	consumer, err := mq.Consume("orders")
	if err != nil {
		t.Fatalf("consume from channel failed: %v", err)
	}

	if err := mq.Publish("orders", []byte("order-1")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-consumer:
		if string(msg) != "order-1" {
			t.Fatalf("unexpected message: %q", string(msg))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	stats := mq.Channels()
	if len(stats) == 0 {
		t.Fatal("expected channel stats")
	}
}
