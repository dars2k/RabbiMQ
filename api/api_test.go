package api

import (
	"RabbiMQ/broker"
	"RabbiMQ/shabbat"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var weekday = time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)

func newTestMQ(t *testing.T) RabbiMQ {
	t.Helper()
	mq := newWithClock(&shabbat.FixedClock{Time: weekday}, Config{})
	t.Cleanup(mq.Shutdown)
	return mq
}

func receive(t *testing.T, ch <-chan []byte) string {
	t.Helper()
	select {
	case msg := <-ch:
		return string(msg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return ""
	}
}

func TestPublishConsumeDefaultChannel(t *testing.T) {
	mq := newTestMQ(t)
	consumer, err := mq.Consume("")
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if err := mq.Publish("", []byte("hello")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if got := receive(t, consumer); got != "hello" {
		t.Fatalf("unexpected message: %q", got)
	}
}

func TestPublishConsumeChannel(t *testing.T) {
	mq := newTestMQ(t)
	consumer, err := mq.Consume("orders")
	if err != nil {
		t.Fatalf("consume from channel failed: %v", err)
	}

	if err := mq.Publish("orders", []byte("order-1")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if got := receive(t, consumer); got != "order-1" {
		t.Fatalf("unexpected message: %q", got)
	}

	stats := mq.Channels()
	if len(stats) != 2 {
		t.Fatalf("expected default and orders channels, got %+v", stats)
	}
	if err := mq.AcquireConsumer(); err != nil {
		t.Fatal(err)
	}
	if mq.Metrics().Consumers != 1 {
		t.Fatalf("unexpected metrics: %+v", mq.Metrics())
	}
	mq.ReleaseConsumer()
	if isShabbat, _ := mq.ShabbatStatus(); isShabbat {
		t.Fatal("did not expect Shabbat on a Monday")
	}
}

func TestNewDefaults(t *testing.T) {
	mq := New()
	defer mq.Shutdown()
	if mq.Metrics().Channels != 1 {
		t.Fatalf("expected default channel only: %+v", mq.Metrics())
	}

	mq2, err := NewWithFile(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	mq2.Shutdown()
}

func TestFileBackedRestoresChannelsAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rabbimq.log")
	clock := &shabbat.FixedClock{Time: weekday}

	mq, err := newWithFileClock(clock, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []string{"o1", "o2"} {
		if err := mq.Publish("orders", []byte(msg)); err != nil {
			t.Fatal(err)
		}
	}
	if err := mq.Publish("", []byte("d1")); err != nil {
		t.Fatal(err)
	}
	consumer, _ := mq.Consume("orders")
	if got := receive(t, consumer); got != "o1" {
		t.Fatalf("expected o1, got %q", got)
	}
	mq.Shutdown()

	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "rabbimq.orders.log")); err != nil {
		t.Fatalf("expected channel log: %v", err)
	}

	mq2, err := newWithFileClock(clock, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer mq2.Shutdown()

	want := []broker.ChannelStats{{Channel: "default", QueueDepth: 1}, {Channel: "orders", QueueDepth: 1}}
	if got := mq2.Channels(); !reflect.DeepEqual(got, want) {
		t.Fatalf("channels after restart = %+v, want %+v", got, want)
	}
	consumer2, _ := mq2.Consume("orders")
	if got := receive(t, consumer2); got != "o2" {
		t.Fatalf("expected o2 (o1 was already consumed), got %q", got)
	}
}

func TestFileBackedRestoreRespectsChannelLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rabbimq.log")
	clock := &shabbat.FixedClock{Time: weekday}
	mq, err := newWithFileClock(clock, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_ = mq.Publish("a", []byte("x"))
	_ = mq.Publish("b", []byte("x"))
	mq.Shutdown()

	_, err = newWithFileClock(clock, path, Config{Limits: broker.Config{MaxChannels: 2}})
	if !errors.Is(err, broker.ErrChannelLimit) {
		t.Fatalf("expected ErrChannelLimit when persisted channels exceed limit, got %v", err)
	}
}

func TestChannelLogPath(t *testing.T) {
	tests := []struct {
		base, channel, want string
	}{
		{"/data/rabbimq.log", "orders", "/data/rabbimq.orders.log"},
		{"/data/queue", "orders", "/data/queue.orders.log"},
		{"/data/.log", "x", "/data/rabbimq.x.log"},
	}
	for _, tc := range tests {
		if got := channelLogPath(tc.base, tc.channel); got != tc.want {
			t.Errorf("channelLogPath(%q, %q) = %q, want %q", tc.base, tc.channel, got, tc.want)
		}
	}
}

func TestPersistedChannelsIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"rabbimq.log",                  // default channel
		"rabbimq.orders.log",           // channel
		"rabbimq.default.log",          // reserved name
		"rabbimq.bad.name.log",         // invalid channel
		"rabbimq.log.compact-123",      // compaction temp file
		"other.orders.log",             // different base
		"rabbimq.payments.log",         // channel
		"rabbimq.orders.log.compact-1", // compaction temp file
	} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := persistedChannels(filepath.Join(dir, "rabbimq.log"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"orders", "payments"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persistedChannels = %v, want %v", got, want)
	}
}

func TestGlobEscape(t *testing.T) {
	if got := globEscape(`a*b?[c]\`); got != `a\*b\?\[c\]\\` {
		t.Fatalf("unexpected escape: %s", got)
	}
}
