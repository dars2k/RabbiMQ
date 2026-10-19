package broker

import (
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockClock is a mock implementation of the Clock interface for testing.
type mockClock struct {
	mu          sync.Mutex
	currentTime time.Time
}

// Now returns the mock time.
func (mc *mockClock) Now() time.Time {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.currentTime
}

// setTime sets the current time of the mock clock.
func (mc *mockClock) setTime(t time.Time) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.currentTime = t
}

var (
	weekday       = time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC) // Monday 10:00 AM
	beforeShabbat = time.Date(2024, 5, 10, 16, 0, 0, 0, time.UTC) // Friday 4:00 PM
	duringShabbat = time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC) // Friday 6:00 PM
	afterShabbat  = time.Date(2024, 5, 11, 21, 0, 0, 0, time.UTC) // Saturday 9:00 PM
)

func newTestBroker(t *testing.T, start time.Time, cfg Config) (*Broker, *mockClock) {
	t.Helper()
	clock := &mockClock{currentTime: start}
	b := NewBrokerWithFactoryConfig(shabbat.NewShabbatClock(clock), queue.NewInMemoryQueue(), nil, cfg)
	t.Cleanup(b.Shutdown)
	return b, clock
}

// setTime moves the clock and applies the change immediately rather than
// waiting for the watcher's next tick.
func setTime(b *Broker, clock *mockClock, now time.Time) {
	clock.setTime(now)
	b.checkShabbat()
}

func receive(t *testing.T, ch <-chan []byte) string {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("consumer channel closed")
		}
		return string(msg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return ""
	}
}

func expectNothing(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("unexpected delivery: %q", msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func eventually(t *testing.T, cond func() bool, msg func() string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBroker(t *testing.T) {
	broker, clock := newTestBroker(t, weekday, Config{})

	t.Run("Publish and Consume (not Shabbat)", func(t *testing.T) {
		if err := broker.Publish("", []byte("task1")); err != nil {
			t.Fatalf("Expected no error when publishing, but got %v", err)
		}

		consumer, err := broker.Consume("")
		if err != nil {
			t.Fatalf("Expected no error when consuming, but got %v", err)
		}
		if got := receive(t, consumer); got != "task1" {
			t.Errorf("Expected to receive task 'task1', but got '%s'", got)
		}
	})

	t.Run("Publish during Shabbat", func(t *testing.T) {
		setTime(broker, clock, duringShabbat)

		err := broker.Publish("", []byte("task2"))
		if !errors.Is(err, ErrShabbatObserved) {
			t.Errorf("Expected ErrShabbatObserved, but got %v", err)
		}
	})

	t.Run("Queue messages before Shabbat, consume after", func(t *testing.T) {
		setTime(broker, clock, beforeShabbat)
		if err := broker.Publish("", []byte("task3")); err != nil {
			t.Fatal(err)
		}
		if err := broker.Publish("", []byte("task4")); err != nil {
			t.Fatal(err)
		}

		setTime(broker, clock, duringShabbat)
		if err := broker.Publish("", []byte("shabbat task")); !errors.Is(err, ErrShabbatObserved) {
			t.Errorf("Expected ErrShabbatObserved, but got %v", err)
		}

		consumer, err := broker.Consume("")
		if err != nil {
			t.Fatalf("Expected no error when consuming, but got %v", err)
		}
		expectNothing(t, consumer)

		m := broker.Metrics()
		if !m.IsShabbat || m.HeldDuringShabbat != 2 {
			t.Fatalf("unexpected metrics during Shabbat: %+v", m)
		}

		setTime(broker, clock, afterShabbat)
		if got := receive(t, consumer); got != "task3" {
			t.Errorf("Expected task3, got %q", got)
		}
		if got := receive(t, consumer); got != "task4" {
			t.Errorf("Expected task4, got %q", got)
		}
	})
}

// Regression: resumeDelivery used to block the Shabbat watcher forever when
// Shabbat ended with no consumer connected, so the next Shabbat was missed.
func TestShabbatEndsWithoutConsumers(t *testing.T) {
	b, clock := newTestBroker(t, beforeShabbat, Config{})
	if err := b.Publish("orders", []byte("held")); err != nil {
		t.Fatal(err)
	}

	setTime(b, clock, duringShabbat)
	setTime(b, clock, afterShabbat)

	done := make(chan struct{})
	go func() {
		// Shabbat starts again a week later.
		setTime(b, clock, duringShabbat.AddDate(0, 0, 7))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shabbat check blocked")
	}

	if err := b.Publish("orders", []byte("x")); !errors.Is(err, ErrShabbatObserved) {
		t.Fatalf("expected ErrShabbatObserved, got %v", err)
	}

	setTime(b, clock, afterShabbat.AddDate(0, 0, 7))
	consumer, _ := b.Consume("orders")
	if got := receive(t, consumer); got != "held" {
		t.Fatalf("expected held task, got %q", got)
	}
}

// A task waiting for a consumer must not be delivered once Shabbat begins.
func TestPendingTaskNotDeliveredWhenShabbatStarts(t *testing.T) {
	b, clock := newTestBroker(t, beforeShabbat, Config{})
	if err := b.Publish("", []byte("waiting")); err != nil {
		t.Fatal(err)
	}
	// Give the delivery goroutine time to block on the send.
	time.Sleep(50 * time.Millisecond)

	setTime(b, clock, duringShabbat)
	consumer, _ := b.Consume("")
	expectNothing(t, consumer)

	setTime(b, clock, afterShabbat)
	if got := receive(t, consumer); got != "waiting" {
		t.Fatalf("expected waiting, got %q", got)
	}
}

func TestShabbatWatcherDetectsTransition(t *testing.T) {
	b, clock := newTestBroker(t, duringShabbat, Config{})
	if !b.IsShabbat() {
		t.Fatal("expected Shabbat at start")
	}
	clock.setTime(afterShabbat)

	deadline := time.Now().Add(3 * shabbatCheckInterval)
	for b.IsShabbat() {
		if time.Now().After(deadline) {
			t.Fatal("watcher did not observe end of Shabbat")
		}
		time.Sleep(10 * time.Millisecond)
	}

	isShabbat, remaining := b.ShabbatStatus()
	if isShabbat || remaining <= 0 {
		t.Fatalf("unexpected status: %v %v", isShabbat, remaining)
	}
}

func TestChannelsAreIsolatedAndOrdered(t *testing.T) {
	b, _ := newTestBroker(t, weekday, Config{})
	for i := 0; i < 5; i++ {
		if err := b.Publish("a", []byte(fmt.Sprintf("a%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := b.Publish("b", []byte(fmt.Sprintf("b%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	stats := b.Channels()
	want := []ChannelStats{{"a", 5}, {"b", 5}, {"default", 0}}
	if fmt.Sprint(stats) != fmt.Sprint(want) {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	ca, _ := b.Consume("a")
	cb, _ := b.Consume("b")
	for i := 0; i < 5; i++ {
		if got := receive(t, ca); got != fmt.Sprintf("a%d", i) {
			t.Fatalf("channel a out of order: %q", got)
		}
		if got := receive(t, cb); got != fmt.Sprintf("b%d", i) {
			t.Fatalf("channel b out of order: %q", got)
		}
	}

	// Accounting happens right after the consumer takes the task.
	eventually(t, func() bool {
		m := b.Metrics()
		return m.Published == 10 && m.Delivered == 10 && m.QueueDepth == 0 && m.Channels == 3
	}, func() string { return fmt.Sprintf("unexpected metrics: %+v", b.Metrics()) })
}

func TestLimits(t *testing.T) {
	t.Run("message size", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxMessageBytes: 4})
		if err := b.Publish("", []byte("12345")); !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("expected ErrMessageTooLarge, got %v", err)
		}
		if err := b.Publish("", []byte("1234")); err != nil {
			t.Fatalf("expected success at limit, got %v", err)
		}
	})

	t.Run("queue depth", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxQueueDepth: 2})
		_ = b.Publish("a", []byte("1"))
		_ = b.Publish("b", []byte("2"))
		if err := b.Publish("c", []byte("3")); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected ErrQueueFull, got %v", err)
		}
	})

	t.Run("channel depth", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxChannelDepth: 1})
		_ = b.Publish("a", []byte("1"))
		if err := b.Publish("a", []byte("2")); !errors.Is(err, ErrChannelFull) {
			t.Fatalf("expected ErrChannelFull, got %v", err)
		}
		if err := b.Publish("b", []byte("1")); err != nil {
			t.Fatalf("other channel should accept, got %v", err)
		}
	})

	t.Run("channel count", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxChannels: 2})
		if err := b.Publish("a", []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err := b.Publish("b", []byte("1")); !errors.Is(err, ErrChannelLimit) {
			t.Fatalf("expected ErrChannelLimit, got %v", err)
		}
		if _, err := b.Consume("b"); !errors.Is(err, ErrChannelLimit) {
			t.Fatalf("expected ErrChannelLimit from Consume, got %v", err)
		}
	})

	t.Run("consumers", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxConsumers: 1})
		if err := b.AcquireConsumer(); err != nil {
			t.Fatal(err)
		}
		if err := b.AcquireConsumer(); !errors.Is(err, ErrConsumerLimit) {
			t.Fatalf("expected ErrConsumerLimit, got %v", err)
		}
		if got := b.Metrics().Consumers; got != 1 {
			t.Fatalf("expected 1 consumer, got %d", got)
		}
		b.ReleaseConsumer()
		b.ReleaseConsumer() // extra release must not go negative
		if err := b.AcquireConsumer(); err != nil {
			t.Fatalf("expected slot after release, got %v", err)
		}
	})

	t.Run("rejections are counted", func(t *testing.T) {
		b, _ := newTestBroker(t, weekday, Config{MaxMessageBytes: 1, MaxChannelDepth: 1})
		_ = b.Publish("", []byte("toolong"))
		_ = b.Publish("", []byte("a"))
		_ = b.Publish("", []byte("b"))
		_ = b.Publish("bad name", []byte("c"))
		if got := b.Metrics().Rejected; got != 3 {
			t.Fatalf("expected 3 rejections, got %d", got)
		}
	})
}

// Concurrent publishers must never push a queue beyond its configured limit.
func TestConcurrentPublishRespectsLimits(t *testing.T) {
	const limit = 50
	b, _ := newTestBroker(t, weekday, Config{MaxQueueDepth: limit})

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Publish("load", []byte("x")); err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if accepted != limit {
		t.Fatalf("accepted %d tasks, limit is %d", accepted, limit)
	}
	if depth := b.Metrics().QueueDepth; depth != limit {
		t.Fatalf("queue depth %d, limit is %d", depth, limit)
	}
}

func TestValidateChannel(t *testing.T) {
	valid := []string{"", "orders", "Orders_2024", "a-b", strings.Repeat("x", MaxChannelNameLength)}
	for _, name := range valid {
		if err := ValidateChannel(name); err != nil {
			t.Errorf("%q should be valid: %v", name, err)
		}
	}
	invalid := []string{"a.b", "a/b", "../etc", "spaced name", "émoji", strings.Repeat("x", MaxChannelNameLength+1)}
	for _, name := range invalid {
		if err := ValidateChannel(name); !errors.Is(err, ErrInvalidChannel) {
			t.Errorf("%q should be invalid, got %v", name, err)
		}
	}
}

func TestShutdown(t *testing.T) {
	clock := &mockClock{currentTime: weekday}
	b := NewBroker(shabbat.NewShabbatClock(clock), queue.NewInMemoryQueue())
	_ = b.Publish("orders", []byte("pending")) // no consumer: delivery goroutine is blocked
	consumer, _ := b.Consume("orders")

	done := make(chan struct{})
	go func() {
		b.Shutdown()
		b.Shutdown() // idempotent
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung while a task was waiting for a consumer")
	}

	if _, ok := <-consumer; ok {
		t.Fatal("expected consumer channel to be closed")
	}
	if err := b.Publish("orders", []byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := b.Consume("new"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Consume, got %v", err)
	}
	if err := b.AcquireConsumer(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from AcquireConsumer, got %v", err)
	}
}

// Tasks waiting for a consumer at shutdown stay in the persistent queue.
func TestShutdownKeepsUndeliveredTasks(t *testing.T) {
	dir := t.TempDir()
	factory := func(channel string) (queue.Queue, error) {
		return queue.NewPersistentQueue(filepath.Join(dir, channel+".log"))
	}
	clock := &mockClock{currentTime: weekday}

	q, err := factory(DefaultChannel)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBrokerWithFactory(shabbat.NewShabbatClock(clock), q, factory)
	for _, s := range []string{"one", "two"} {
		if err := b.Publish("", []byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	consumer, _ := b.Consume("")
	if got := receive(t, consumer); got != "one" {
		t.Fatalf("expected one, got %q", got)
	}
	b.Shutdown()

	q2, err := factory(DefaultChannel)
	if err != nil {
		t.Fatal(err)
	}
	b2 := NewBrokerWithFactory(shabbat.NewShabbatClock(clock), q2, factory)
	defer b2.Shutdown()
	if depth := b2.Metrics().QueueDepth; depth != 1 {
		t.Fatalf("expected 1 pending task after restart, got %d", depth)
	}
	consumer2, _ := b2.Consume("")
	if got := receive(t, consumer2); got != "two" {
		t.Fatalf("expected two, got %q", got)
	}
}

func TestQueueFactoryError(t *testing.T) {
	clock := &mockClock{currentTime: weekday}
	boom := errors.New("boom")
	b := NewBrokerWithFactory(shabbat.NewShabbatClock(clock), queue.NewInMemoryQueue(), func(string) (queue.Queue, error) {
		return nil, boom
	})
	defer b.Shutdown()

	if err := b.Publish("orders", []byte("x")); !errors.Is(err, boom) {
		t.Fatalf("expected factory error, got %v", err)
	}
	if err := b.EnsureChannel("orders"); !errors.Is(err, boom) {
		t.Fatalf("expected factory error from EnsureChannel, got %v", err)
	}
}

func TestConcurrentChannelCreationClosesLoser(t *testing.T) {
	clock := &mockClock{currentTime: weekday}
	var mu sync.Mutex
	var created []*countingQueue
	b := NewBrokerWithFactory(shabbat.NewShabbatClock(clock), queue.NewInMemoryQueue(), func(string) (queue.Queue, error) {
		q := &countingQueue{Queue: queue.NewInMemoryQueue()}
		mu.Lock()
		created = append(created, q)
		mu.Unlock()
		return q, nil
	})
	defer b.Shutdown()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.EnsureChannel("race")
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	open := 0
	for _, q := range created {
		if !q.isClosed() {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("expected exactly one open queue, got %d of %d", open, len(created))
	}
}

type countingQueue struct {
	queue.Queue
	mu     sync.Mutex
	closed bool
}

func (q *countingQueue) Close() error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	return q.Queue.Close()
}

func (q *countingQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("zero config should be valid: %v", err)
	}
	if err := (Config{MaxConsumers: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative limit")
	}
	if got := (Config{}).Normalize().MaxMessageBytes; got != DefaultMaxMessageBytes {
		t.Fatalf("unexpected default: %d", got)
	}
}
