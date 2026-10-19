package queue

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// ackMarker is written in place of a length prefix to record that the task at
// the front of the queue was removed. Tasks can never be this large because
// the length prefix is a uint32 and the marker is reserved.
const ackMarker = math.MaxUint32

// maxTaskSize is the largest task the on-disk format can represent.
const maxTaskSize = ackMarker - 1

// ErrTaskTooLarge is returned when a task cannot be represented in the log format.
var ErrTaskTooLarge = errors.New("task too large for persistent queue")

// ErrQueueClosed is returned when operating on a closed persistent queue.
var ErrQueueClosed = errors.New("queue is closed")

// PersistentQueue is a thread-safe, disk-backed, FIFO queue.
//
// The on-disk format is an append-only log of records. Each record starts
// with a big-endian uint32. A value of ackMarker means "the task at the front
// was removed"; any other value is the length of the task payload that
// follows. The log is compacted every time the queue is opened.
type PersistentQueue struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	mem    Queue
	closed bool
}

// NewPersistentQueue creates a new PersistentQueue.
// It loads pending tasks from the given file path and compacts the log.
func NewPersistentQueue(filePath string) (Queue, error) {
	tasks, err := readLog(filePath)
	if err != nil {
		return nil, err
	}

	// Rewrite the log so that acknowledged tasks and any truncated tail
	// left behind by a crash do not accumulate.
	if err := writeCompacted(filePath, tasks); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	memQueue := NewInMemoryQueue()
	for _, task := range tasks {
		if err := memQueue.Enqueue(task); err != nil {
			file.Close()
			return nil, err
		}
	}

	return &PersistentQueue{
		path: filePath,
		file: file,
		mem:  memQueue,
	}, nil
}

// readLog replays the log at path and returns the tasks that are still pending.
// A missing file yields an empty queue. A truncated trailing record (for
// example after a crash mid-write) is ignored.
func readLog(path string) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var tasks [][]byte
	head := 0
	for {
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}

		if length == ackMarker {
			if head < len(tasks) {
				tasks[head] = nil
				head++
			}
			continue
		}

		task := make([]byte, length)
		if _, err := io.ReadFull(reader, task); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks[head:], nil
}

// writeCompacted atomically replaces the log at path with one containing only tasks.
func writeCompacted(path string, tasks [][]byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".compact-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}

	writer := bufio.NewWriter(tmp)
	for _, task := range tasks {
		if _, err := writer.Write(encodeTask(task)); err != nil {
			return cleanup(err)
		}
	}
	if err := writer.Flush(); err != nil {
		return cleanup(err)
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(err)
	}
	if err := tmp.Chmod(0644); err != nil {
		return cleanup(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("compact queue log: %w", err)
	}
	return nil
}

// encodeTask returns the length-prefixed record for task.
func encodeTask(task []byte) []byte {
	record := make([]byte, 4+len(task))
	binary.BigEndian.PutUint32(record, uint32(len(task)))
	copy(record[4:], task)
	return record
}

// Enqueue adds a task to the end of the queue and persists it to disk.
func (pq *PersistentQueue) Enqueue(task []byte) error {
	if uint64(len(task)) > maxTaskSize {
		return ErrTaskTooLarge
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return ErrQueueClosed
	}

	// Write the whole record in one call so a crash leaves at most a
	// truncated tail, which readLog discards.
	if _, err := pq.file.Write(encodeTask(task)); err != nil {
		return err
	}

	return pq.mem.Enqueue(task)
}

// Peek returns the task at the front of the queue without removing it.
func (pq *PersistentQueue) Peek() ([]byte, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.mem.Peek()
}

// Dequeue removes and returns the task from the front of the queue.
// The removal is recorded in the log so the task is not replayed on restart.
func (pq *PersistentQueue) Dequeue() ([]byte, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return nil, false
	}

	task, ok := pq.mem.Dequeue()
	if !ok {
		return nil, false
	}

	var marker [4]byte
	binary.BigEndian.PutUint32(marker[:], ackMarker)
	// If the ack cannot be written the task will be redelivered after a
	// restart (at-least-once), which is preferable to losing it.
	_, _ = pq.file.Write(marker[:])
	return task, true
}

// Size returns the number of pending tasks.
func (pq *PersistentQueue) Size() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.mem.Size()
}

// Close closes the queue's file handle. It is safe to call more than once.
func (pq *PersistentQueue) Close() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return nil
	}
	pq.closed = true
	return pq.file.Close()
}
