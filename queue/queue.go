package queue

import (
	"sync"
)

// Queue is an interface for a thread-safe, FIFO queue.
type Queue interface {
	Enqueue(task []byte) error
	Dequeue() ([]byte, bool)
	Size() int
	Close() error
}

// inMemoryQueue is a thread-safe, in-memory, FIFO queue for tasks.
type inMemoryQueue struct {
	mu    sync.Mutex
	tasks [][]byte
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

// Dequeue removes and returns the task from the front of the queue.
// It returns the task and true if the queue is not empty, otherwise it returns nil and false.
func (q *inMemoryQueue) Dequeue() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tasks) == 0 {
		return nil, false
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task, true
}

// Size returns the current number of tasks in the queue.
func (q *inMemoryQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// Close is a no-op for the in-memory queue.
func (q *inMemoryQueue) Close() error {
	return nil
}
