package broker

import (
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
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

func TestBroker(t *testing.T) {
	mockClock := &mockClock{}
	shabbatClock := shabbat.NewShabbatClock(mockClock)
	q := queue.NewInMemoryQueue()
	broker := NewBroker(shabbatClock, q)
	defer broker.Shutdown()

	// Not Shabbat
	mockClock.setTime(time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC)) // Monday 10:00 AM

	t.Run("Publish and Consume (not Shabbat)", func(t *testing.T) {
		task := []byte("task1")
		err := broker.Publish(task)
		if err != nil {
			t.Fatalf("Expected no error when publishing, but got %v", err)
		}

		consumer := broker.Consume()
		receivedTask := <-consumer

		if string(receivedTask) != string(task) {
			t.Errorf("Expected to receive task '%s', but got '%s'", task, receivedTask)
		}
	})

	t.Run("Publish during Shabbat", func(t *testing.T) {
		// Set time to Shabbat
		mockClock.setTime(time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC)) // Friday 6:00 PM
		time.Sleep(2 * time.Second) // allow shabbat watcher to update

		task := []byte("task2")
		err := broker.Publish(task)
		if err != ErrShabbatObserved {
			t.Errorf("Expected ErrShabbatObserved, but got %v", err)
		}
	})

	t.Run("Queue messages before Shabbat, consume after", func(t *testing.T) {
		// Set time before Shabbat
		mockClock.setTime(time.Date(2024, 5, 10, 16, 0, 0, 0, time.UTC)) // Friday 4:00 PM
		time.Sleep(2 * time.Second) // allow shabbat watcher to update

		task3 := []byte("task3")
		task4 := []byte("task4")
		broker.Publish(task3)
		broker.Publish(task4)

		// Set time to Shabbat
		mockClock.setTime(time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC)) // Friday 6:00 PM
		time.Sleep(2 * time.Second) // allow shabbat watcher to update

		// Try to publish during shabbat, should fail
		err := broker.Publish([]byte("shabbat task"))
		if err != ErrShabbatObserved {
			t.Errorf("Expected ErrShabbatObserved, but got %v", err)
		}

		// Set time after Shabbat
		mockClock.setTime(time.Date(2024, 5, 11, 21, 0, 0, 0, time.UTC)) // Saturday 9:00 PM
		time.Sleep(2 * time.Second) // allow shabbat watcher to update

		consumer := broker.Consume()
		receivedTask3 := <-consumer
		if string(receivedTask3) != string(task3) {
			t.Errorf("Expected to receive task '%s', but got '%s'", task3, receivedTask3)
		}
		receivedTask4 := <-consumer
		if string(receivedTask4) != string(task4) {
			t.Errorf("Expected to receive task '%s', but got '%s'", task4, receivedTask4)
		}
	})
}
