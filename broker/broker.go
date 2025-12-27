package broker

import (
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"errors"
	"sync"
	"time"
)

var (
	// ErrShabbatObserved is returned when a client tries to publish a message during Shabbat.
	ErrShabbatObserved = errors.New("cannot publish messages on Shabbat")
)

// Broker is a message broker that observes Shabbat.
type Broker struct {
	shabbatClock *shabbat.ShabbatClock
	queue        *queue.InMemoryQueue
	mu           sync.Mutex
	isShabbat    bool
	taskChannel  chan []byte
	wg           sync.WaitGroup
	shutdown     chan struct{}
}

// NewBroker creates a new Broker.
func NewBroker(shabbatClock *shabbat.ShabbatClock, queue *queue.InMemoryQueue) *Broker {
	b := &Broker{
		shabbatClock: shabbatClock,
		queue:        queue,
		isShabbat:    shabbatClock.IsShabbat(),
		taskChannel:  make(chan []byte),
		shutdown:     make(chan struct{}),
	}

	b.wg.Add(1)
	go b.shabbatWatcher()

	b.wg.Add(1)
	go b.deliveryManager()

	return b
}

// Publish adds a task to the broker.
// It returns ErrShabbatObserved if it is currently Shabbat.
func (b *Broker) Publish(task []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isShabbat {
		return ErrShabbatObserved
	}

	b.queue.Enqueue(task)
	// Non-blocking send to notify the delivery manager of a new task.
	select {
	case b.taskChannel <- task:
	default:
	}
	return nil
}

// Consume returns a channel that receives tasks.
func (b *Broker) Consume() <-chan []byte {
	return b.taskChannel
}

// shabbatWatcher monitors for changes in Shabbat state.
func (b *Broker) shabbatWatcher() {
	defer b.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wasShabbat := b.isShabbat
			b.mu.Lock()
			b.isShabbat = b.shabbatClock.IsShabbat()
			b.mu.Unlock()

			if wasShabbat && !b.isShabbat {
				// Shabbat has ended, resume delivery
				b.resumeDelivery()
			}
		case <-b.shutdown:
			return
		}
	}
}

// deliveryManager is responsible for delivering tasks to consumers.
func (b *Broker) deliveryManager() {
	defer b.wg.Done()
	for {
		select {
		case <-b.shutdown:
			return
		default:
			b.mu.Lock()
			if b.isShabbat {
				b.mu.Unlock()
				// Block until Shabbat is over
				time.Sleep(1 * time.Second) // Check again in a second
				continue
			}

			task, ok := b.queue.Dequeue()
			b.mu.Unlock()

			if ok {
				b.taskChannel <- task
			} else {
				// Wait for a new task to be published
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// resumeDelivery sends all pending tasks from the queue to the task channel.
func (b *Broker) resumeDelivery() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		task, ok := b.queue.Dequeue()
		if !ok {
			break
		}
		b.taskChannel <- task
	}
}

// Shutdown gracefully shuts down the broker.
func (b *Broker) Shutdown() {
	close(b.shutdown)
	b.wg.Wait()
	close(b.taskChannel)
}
