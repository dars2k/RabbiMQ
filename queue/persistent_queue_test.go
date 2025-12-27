package queue

import (
	"bytes"
	"os"
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
