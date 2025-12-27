package queue

import (
	"sync"
)

// InMemoryQueue is a thread-safe, in-memory, FIFO queue for tasks.
type InMemoryQueue struct {
	mu    sync.Mutex
	tasks [][]byte
}

// NewInMemoryQueue creates a new InMemoryQueue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		tasks: make([][]byte, 0),
	}
}

// Enqueue adds a task to the end of the queue.
func (q *InMemoryQueue) Enqueue(task []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
}

// Dequeue removes and returns the task from the front of the queue.
// It returns the task and true if the queue is not empty, otherwise it returns nil and false.
func (q *InMemoryQueue) Dequeue() ([]byte, bool) {
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
func (q *InMemoryQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}
