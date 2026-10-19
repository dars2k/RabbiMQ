package queue

import (
	"sync"
)

// Queue is an interface for a thread-safe, FIFO queue.
type Queue interface {
	Enqueue(task []byte) error
	// Peek returns the task at the front of the queue without removing it.
	Peek() ([]byte, bool)
	Dequeue() ([]byte, bool)
	Size() int
	Close() error
}

// inMemoryQueue is a thread-safe, in-memory, FIFO queue for tasks.
type inMemoryQueue struct {
	mu    sync.Mutex
	tasks [][]byte
	head  int
}

// NewInMemoryQueue creates a new inMemoryQueue.
func NewInMemoryQueue() Queue {
	return &inMemoryQueue{
		tasks: make([][]byte, 0),
	}
}

// Enqueue adds a task to the end of the queue.
func (q *inMemoryQueue) Enqueue(task []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	return nil
}

// Peek returns the task at the front of the queue without removing it.
func (q *inMemoryQueue) Peek() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head >= len(q.tasks) {
		return nil, false
	}
	return q.tasks[q.head], true
}

// Dequeue removes and returns the task from the front of the queue.
// It returns the task and true if the queue is not empty, otherwise it returns nil and false.
func (q *inMemoryQueue) Dequeue() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head >= len(q.tasks) {
		return nil, false
	}

	task := q.tasks[q.head]
	// Drop the reference so the payload can be garbage collected.
	q.tasks[q.head] = nil
	q.head++

	// Reclaim the backing array once the consumed prefix dominates it.
	if q.head == len(q.tasks) {
		q.tasks = q.tasks[:0]
		q.head = 0
	} else if q.head >= 64 && q.head*2 >= len(q.tasks) {
		remaining := make([][]byte, len(q.tasks)-q.head)
		copy(remaining, q.tasks[q.head:])
		q.tasks = remaining
		q.head = 0
	}
	return task, true
}

// Size returns the current number of tasks in the queue.
func (q *inMemoryQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) - q.head
}

// Close is a no-op for the in-memory queue.
func (q *inMemoryQueue) Close() error {
	return nil
}
