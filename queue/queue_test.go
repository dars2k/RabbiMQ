package queue

import (
	"bytes"
	"sync"
	"testing"
)

func TestQueue(t *testing.T) {
	q := NewInMemoryQueue()

	// Test case 1: Enqueue and Dequeue a single item
	t.Run("Single Item", func(t *testing.T) {
		task := []byte("task1")
		err := q.Enqueue(task)
		if err != nil {
			t.Fatalf("Expected no error when enqueuing, but got %v", err)
		}

		if q.Size() != 1 {
			t.Errorf("Expected queue size to be 1, but got %d", q.Size())
		}

		dequeuedTask, ok := q.Dequeue()
		if !ok {
			t.Errorf("Expected to dequeue a task, but queue was empty")
		}

		if !bytes.Equal(task, dequeuedTask) {
			t.Errorf("Expected dequeued task to be %s, but got %s", task, dequeuedTask)
		}

		if q.Size() != 0 {
			t.Errorf("Expected queue size to be 0, but got %d", q.Size())
		}
	})

	// Test case 2: Dequeue from an empty queue
	t.Run("Dequeue from empty", func(t *testing.T) {
		// ensure queue is empty before this test
		for q.Size() > 0 {
			q.Dequeue()
		}
		_, ok := q.Dequeue()
		if ok {
			t.Errorf("Expected no task to be dequeued from an empty queue")
		}
	})

	// Test case 3: FIFO order
	t.Run("FIFO order", func(t *testing.T) {
		task1 := []byte("task1")
		task2 := []byte("task2")

		err := q.Enqueue(task1)
		if err != nil {
			t.Fatalf("Expected no error when enqueuing, but got %v", err)
		}
		err = q.Enqueue(task2)
		if err != nil {
			t.Fatalf("Expected no error when enqueuing, but got %v", err)
		}

		dequeuedTask1, _ := q.Dequeue()
		if !bytes.Equal(task1, dequeuedTask1) {
			t.Errorf("Expected first dequeued task to be %s, but got %s", task1, dequeuedTask1)
		}

		dequeuedTask2, _ := q.Dequeue()
		if !bytes.Equal(task2, dequeuedTask2) {
			t.Errorf("Expected second dequeued task to be %s, but got %s", task2, dequeuedTask2)
		}
	})
}

func TestQueueConcurrency(t *testing.T) {
	q := NewInMemoryQueue()
	numGoroutines := 100
	numTasksPerGoroutine := 10
	var wg sync.WaitGroup

	// Enqueue concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numTasksPerGoroutine; j++ {
				err := q.Enqueue([]byte("task"))
				if err != nil {
					t.Errorf("Expected no error when enqueuing, but got %v", err)
				}
			}
		}()
	}
	wg.Wait()

	expectedSize := numGoroutines * numTasksPerGoroutine
	if q.Size() != expectedSize {
		t.Errorf("Expected queue size to be %d, but got %d", expectedSize, q.Size())
	}

	// Dequeue concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numTasksPerGoroutine; j++ {
				q.Dequeue()
			}
		}()
	}
	wg.Wait()

	if q.Size() != 0 {
		t.Errorf("Expected queue size to be 0 after concurrent dequeues, but got %d", q.Size())
	}
}

func TestQueuePeek(t *testing.T) {
	q := NewInMemoryQueue()
	if _, ok := q.Peek(); ok {
		t.Fatal("expected empty peek")
	}
	_ = q.Enqueue([]byte("a"))
	_ = q.Enqueue([]byte("b"))

	got, ok := q.Peek()
	if !ok || string(got) != "a" {
		t.Fatalf("unexpected peek: %q %v", got, ok)
	}
	if q.Size() != 2 {
		t.Fatalf("peek must not remove, size=%d", q.Size())
	}
	got, _ = q.Dequeue()
	if string(got) != "a" {
		t.Fatalf("unexpected dequeue: %q", got)
	}
	got, _ = q.Peek()
	if string(got) != "b" {
		t.Fatalf("unexpected peek after dequeue: %q", got)
	}
}

func TestQueueReclaimsBackingArray(t *testing.T) {
	q := NewInMemoryQueue().(*inMemoryQueue)
	for i := 0; i < 1000; i++ {
		_ = q.Enqueue([]byte{byte(i)})
	}
	for i := 0; i < 900; i++ {
		got, ok := q.Dequeue()
		if !ok || got[0] != byte(i) {
			t.Fatalf("dequeue %d: got %v %v", i, got, ok)
		}
	}
	if q.Size() != 100 {
		t.Fatalf("unexpected size: %d", q.Size())
	}
	if len(q.tasks) > 500 {
		t.Fatalf("backing array not compacted: len=%d head=%d", len(q.tasks), q.head)
	}
	for i := 900; i < 1000; i++ {
		got, _ := q.Dequeue()
		if got[0] != byte(i) {
			t.Fatalf("order broken at %d: %v", i, got)
		}
	}
	if q.Size() != 0 {
		t.Fatalf("expected empty queue, size=%d", q.Size())
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
