package queue

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// PersistentQueue is a thread-safe, disk-backed, FIFO queue.
type PersistentQueue struct {
	mu   sync.Mutex
	file *os.File
	// embedding the in-memory queue to handle the task management in memory
	Queue
}

// NewPersistentQueue creates a new PersistentQueue.
// It loads existing tasks from the given file path.
func NewPersistentQueue(filePath string) (Queue, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Create an in-memory queue to hold the tasks
	memQueue := NewInMemoryQueue()

	// Load tasks from the file
	if err := loadTasks(file, memQueue); err != nil {
		file.Close()
		return nil, err
	}

	return &PersistentQueue{
		file:  file,
		Queue: memQueue,
	}, nil
}

// loadTasks reads tasks from the file and loads them into the in-memory queue.
func loadTasks(file *os.File, q Queue) error {
	_, err := file.Seek(0, 0)
	if err != nil {
		return err
	}

	reader := io.Reader(file)
	for {
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return err
		}

		task := make([]byte, length)
		if _, err := io.ReadFull(reader, task); err != nil {
			return err
		}

		if err := q.Enqueue(task); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue adds a task to the end of the queue and persists it to disk.
func (pq *PersistentQueue) Enqueue(task []byte) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Write to disk first
	length := uint32(len(task))
	if err := binary.Write(pq.file, binary.BigEndian, length); err != nil {
		return err
	}
	if _, err := pq.file.Write(task); err != nil {
		return err
	}

	// Then add to in-memory queue
	return pq.Queue.Enqueue(task)
}

// Dequeue removes and returns the task from the front of the queue.
// The on-disk log is not modified on dequeue. Compaction happens on startup.
func (pq *PersistentQueue) Dequeue() ([]byte, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.Queue.Dequeue()
}

// Close closes the queue's file handle.
func (pq *PersistentQueue) Close() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.file.Close()
}
