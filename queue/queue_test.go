package queue

import (
	"bytes"
	"sync"
	"testing"
)

func TestInMemoryQueue(t *testing.T) {
	q := NewInMemoryQueue()

	// Test case 1: Enqueue and Dequeue a single item
	t.Run("Single Item", func(t *testing.T) {
		task := []byte("task1")
		q.Enqueue(task)

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
		_, ok := q.Dequeue()
		if ok {
			t.Errorf("Expected no task to be dequeued from an empty queue")
		}
	})

	// Test case 3: FIFO order
	t.Run("FIFO order", func(t *testing.T) {
		task1 := []byte("task1")
		task2 := []byte("task2")

		q.Enqueue(task1)
		q.Enqueue(task2)

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

func TestInMemoryQueueConcurrency(t *testing.T) {
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
				q.Enqueue([]byte("task"))
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
