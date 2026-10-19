package queue

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentQueue(t *testing.T) {
	// Create a temporary file for the test
	tmpfile, err := os.CreateTemp("", "persistent_queue_test.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// Tasks to be enqueued
	tasks := [][]byte{
		[]byte("task1"),
		[]byte("task2"),
		[]byte("task3"),
	}

	// --- First run: Enqueue tasks ---
	q1, err := NewPersistentQueue(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create persistent queue: %v", err)
	}

	for _, task := range tasks {
		if err := q1.Enqueue(task); err != nil {
			t.Fatalf("Failed to enqueue task: %v", err)
		}
	}

	if q1.Size() != len(tasks) {
		t.Errorf("Expected queue size to be %d, but got %d", len(tasks), q1.Size())
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	// --- Second run: Re-load the queue from the file ---
	q2, err := NewPersistentQueue(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create persistent queue on second run: %v", err)
	}
	defer q2.Close()

	if q2.Size() != len(tasks) {
		t.Errorf("Expected queue size to be %d after reload, but got %d", len(tasks), q2.Size())
	}

	// Dequeue and check the order
	for i, expectedTask := range tasks {
		dequeuedTask, ok := q2.Dequeue()
		if !ok {
			t.Fatalf("Expected to dequeue a task, but queue was empty at index %d", i)
		}
		if !bytes.Equal(expectedTask, dequeuedTask) {
			t.Errorf("Expected dequeued task to be %s, but got %s", expectedTask, dequeuedTask)
		}
	}

	if q2.Size() != 0 {
		t.Errorf("Expected queue to be empty after dequeueing all tasks, but size is %d", q2.Size())
	}
}

func TestPersistentQueueDoesNotReplayConsumedTasks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")

	q1, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"a", "b", "c"} {
		if err := q1.Enqueue([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := q1.Dequeue(); string(got) != "a" {
		t.Fatalf("unexpected dequeue: %q", got)
	}
	if got, _ := q1.Peek(); string(got) != "b" {
		t.Fatalf("unexpected peek: %q", got)
	}
	q1.Close()

	q2, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	if q2.Size() != 2 {
		t.Fatalf("expected 2 pending tasks after restart, got %d", q2.Size())
	}
	if got, _ := q2.Dequeue(); string(got) != "b" {
		t.Fatalf("expected b, got %q", got)
	}
	q2.Close()

	// Reopening compacts the log down to the single pending task.
	q3, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 4+1 {
		t.Fatalf("expected compacted log of 5 bytes, got %d", info.Size())
	}
	if got, _ := q3.Dequeue(); string(got) != "c" {
		t.Fatalf("expected c, got %q", got)
	}
}

func TestPersistentQueueRecoversTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")
	q1, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = q1.Enqueue([]byte("complete"))
	q1.Close()

	// Simulate a crash in the middle of writing a second record.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0, 0, 0, 10, 'p', 'a', 'r'})
	f.Close()

	q2, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatalf("expected recovery from truncated tail, got %v", err)
	}
	defer q2.Close()
	if q2.Size() != 1 {
		t.Fatalf("expected 1 task, got %d", q2.Size())
	}
	if got, _ := q2.Dequeue(); string(got) != "complete" {
		t.Fatalf("unexpected task: %q", got)
	}
}

func TestPersistentQueueClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")
	q, err := NewPersistentQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second close should be a no-op, got %v", err)
	}
	if err := q.Enqueue([]byte("x")); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("expected no dequeue from closed queue")
	}
}

func TestPersistentQueueOpenError(t *testing.T) {
	dir := t.TempDir()
	// A directory cannot be read as a log file.
	if _, err := NewPersistentQueue(dir); err == nil {
		t.Fatal("expected error opening a directory as a queue")
	}
	// A missing parent directory cannot hold the compacted log.
	if _, err := NewPersistentQueue(filepath.Join(dir, "missing", "q.log")); err == nil {
		t.Fatal("expected error for missing parent directory")
	}
}
