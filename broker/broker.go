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
	// ErrQueueFull is returned when the total queue limit is reached.
	ErrQueueFull = errors.New("queue limit reached")
	// ErrChannelFull is returned when a channel queue limit is reached.
	ErrChannelFull = errors.New("channel limit reached")
	// ErrChannelLimit is returned when the channel count limit is reached.
	ErrChannelLimit = errors.New("channel limit reached")
	// ErrConsumerLimit is returned when the consumer limit is reached.
	ErrConsumerLimit = errors.New("consumer limit reached")
	// ErrMessageTooLarge is returned when a message exceeds the size limit.
	ErrMessageTooLarge = errors.New("message exceeds size limit")
)

// Broker is a message broker that observes Shabbat.
type Broker struct {
	shabbatClock *shabbat.ShabbatClock
	mu           sync.Mutex
	isShabbat    bool
	wg           sync.WaitGroup
	shutdown     chan struct{}
	metrics      *Metrics
	channels     map[string]*channelState
	defaultQueue queue.Queue
	queueFactory QueueFactory
	config       Config
	totalDepth   int
	consumers    int
}

// QueueFactory creates a queue for a channel.
type QueueFactory func(channel string) (queue.Queue, error)

type channelState struct {
	queue   queue.Queue
	taskCh  chan []byte
	notify  chan struct{}
	channel string
}

const defaultChannel = "default"

// NewBroker creates a new Broker with a default queue.
func NewBroker(shabbatClock *shabbat.ShabbatClock, queue queue.Queue) *Broker {
	return NewBrokerWithFactory(shabbatClock, queue, nil)
}

// NewBrokerWithFactory creates a new Broker with a queue factory for channels.
func NewBrokerWithFactory(shabbatClock *shabbat.ShabbatClock, queue queue.Queue, factory QueueFactory) *Broker {
	return NewBrokerWithFactoryConfig(shabbatClock, queue, factory, Config{})
}

// NewBrokerWithFactoryConfig creates a new Broker with a queue factory and config.
func NewBrokerWithFactoryConfig(shabbatClock *shabbat.ShabbatClock, queue queue.Queue, factory QueueFactory, config Config) *Broker {
	config = config.normalize()
	b := &Broker{
		shabbatClock: shabbatClock,
		isShabbat:    shabbatClock.IsShabbat(),
		shutdown:     make(chan struct{}),
		metrics:      &Metrics{},
		channels:     make(map[string]*channelState),
		defaultQueue: queue,
		queueFactory: factory,
		config:       config,
	}

	defaultState := &channelState{
		queue:   queue,
		taskCh:  make(chan []byte),
		notify:  make(chan struct{}, 1),
		channel: defaultChannel,
	}
	b.channels[defaultChannel] = defaultState
	b.totalDepth = queue.Size()
	b.wg.Add(1)
	go b.deliveryManager(defaultState)

	b.wg.Add(1)
	go b.shabbatWatcher()

	return b
}

// Publish adds a task to the broker under a specific channel.
// It returns ErrShabbatObserved if it is currently Shabbat.
func (b *Broker) Publish(channel string, task []byte) error {
	if len(task) > b.config.MaxMessageBytes {
		return ErrMessageTooLarge
	}

	b.mu.Lock()
	if b.isShabbat {
		b.metrics.incRejected()
		b.mu.Unlock()
		return ErrShabbatObserved
	}
	b.mu.Unlock()

	state, err := b.getOrCreateChannel(channel)
	if err != nil {
		return err
	}

	b.mu.Lock()
	if b.config.MaxQueueDepth > 0 && b.totalDepth >= b.config.MaxQueueDepth {
		b.mu.Unlock()
		return ErrQueueFull
	}
	if b.config.MaxChannelDepth > 0 && state.queue.Size() >= b.config.MaxChannelDepth {
		b.mu.Unlock()
		return ErrChannelFull
	}
	b.mu.Unlock()

	if err := state.queue.Enqueue(task); err != nil {
		return err
	}
	b.metrics.incPublished()
	b.mu.Lock()
	b.totalDepth++
	b.mu.Unlock()
	// Non-blocking notify to wake the delivery manager.
	select {
	case state.notify <- struct{}{}:
	default:
	}
	return nil
}

// Consume returns a channel that receives tasks for a specific channel.
func (b *Broker) Consume(channel string) (<-chan []byte, error) {
	state, err := b.getOrCreateChannel(channel)
	if err != nil {
		closed := make(chan []byte)
		close(closed)
		return closed, err
	}
	return state.taskCh, nil
}

// shabbatWatcher monitors for changes in Shabbat state.
func (b *Broker) shabbatWatcher() {
	defer b.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			wasShabbat := b.isShabbat
			b.isShabbat = b.shabbatClock.IsShabbat()
			isShabbat := b.isShabbat
			b.mu.Unlock()

			if wasShabbat && !isShabbat {
				// Shabbat has ended, resume delivery
				b.resumeDelivery()
			}
		case <-b.shutdown:
			return
		}
	}
}

// deliveryManager is responsible for delivering tasks to consumers per channel.
func (b *Broker) deliveryManager(state *channelState) {
	defer b.wg.Done()
	for {
		select {
		case <-b.shutdown:
			return
		default:
		}

		b.mu.Lock()
		isShabbat := b.isShabbat
		b.mu.Unlock()

		if isShabbat {
			time.Sleep(1 * time.Second)
			continue
		}

		task, ok := state.queue.Dequeue()
		if ok {
			b.metrics.incDelivered()
			b.mu.Lock()
			if b.totalDepth > 0 {
				b.totalDepth--
			}
			b.mu.Unlock()
			state.taskCh <- task
			continue
		}

		select {
		case <-state.notify:
		case <-b.shutdown:
			return
		}
	}
}

// resumeDelivery sends all pending tasks from the queues to their task channels.
func (b *Broker) resumeDelivery() {
	b.mu.Lock()
	states := make([]*channelState, 0, len(b.channels))
	for _, state := range b.channels {
		states = append(states, state)
	}
	b.mu.Unlock()

	for _, state := range states {
		for {
			task, ok := state.queue.Dequeue()
			if !ok {
				break
			}
			b.metrics.incDelivered()
			b.mu.Lock()
			if b.totalDepth > 0 {
				b.totalDepth--
			}
			b.mu.Unlock()
			state.taskCh <- task
		}
	}
}

// Shutdown gracefully shuts down the broker.
func (b *Broker) Shutdown() {
	close(b.shutdown)
	b.wg.Wait()
	b.mu.Lock()
	states := make([]*channelState, 0, len(b.channels))
	for _, state := range b.channels {
		states = append(states, state)
	}
	b.mu.Unlock()

	for _, state := range states {
		close(state.taskCh)
		state.queue.Close()
	}
}

// Metrics returns a snapshot of broker metrics.
func (b *Broker) Metrics() MetricsSnapshot {
	b.mu.Lock()
	isShabbat := b.isShabbat
	states := make([]*channelState, 0, len(b.channels))
	for _, state := range b.channels {
		states = append(states, state)
	}
	b.mu.Unlock()

	queueDepth := 0
	for _, state := range states {
		queueDepth += state.queue.Size()
	}
	return b.metrics.snapshot(queueDepth, isShabbat)
}

// Channels returns per-channel queue depth information.
func (b *Broker) Channels() []ChannelStats {
	b.mu.Lock()
	states := make([]*channelState, 0, len(b.channels))
	for _, state := range b.channels {
		states = append(states, state)
	}
	b.mu.Unlock()

	stats := make([]ChannelStats, 0, len(states))
	for _, state := range states {
		stats = append(stats, ChannelStats{
			Channel:    state.channel,
			QueueDepth: state.queue.Size(),
		})
	}
	return stats
}

func (b *Broker) getOrCreateChannel(channel string) (*channelState, error) {
	name := channel
	if name == "" {
		name = defaultChannel
	}

	b.mu.Lock()
	if state, ok := b.channels[name]; ok {
		b.mu.Unlock()
		return state, nil
	}
	if b.config.MaxChannels > 0 && len(b.channels) >= b.config.MaxChannels {
		b.mu.Unlock()
		return nil, ErrChannelLimit
	}
	b.mu.Unlock()

	var q queue.Queue
	var err error
	if name == defaultChannel {
		q = b.defaultQueue
	} else if b.queueFactory != nil {
		q, err = b.queueFactory(name)
		if err != nil {
			return nil, err
		}
	} else {
		q = queue.NewInMemoryQueue()
	}

	state := &channelState{
		queue:   q,
		taskCh:  make(chan []byte),
		notify:  make(chan struct{}, 1),
		channel: name,
	}

	b.mu.Lock()
	if existing, ok := b.channels[name]; ok {
		b.mu.Unlock()
		return existing, nil
	}
	b.channels[name] = state
	b.totalDepth += state.queue.Size()
	b.wg.Add(1)
	go b.deliveryManager(state)
	b.mu.Unlock()

	return state, nil
}

// AcquireConsumer reserves a consumer slot if available.
func (b *Broker) AcquireConsumer() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.config.MaxConsumers > 0 && b.consumers >= b.config.MaxConsumers {
		return ErrConsumerLimit
	}
	b.consumers++
	return nil
}

// ReleaseConsumer releases a previously acquired consumer slot.
func (b *Broker) ReleaseConsumer() {
	b.mu.Lock()
	if b.consumers > 0 {
		b.consumers--
	}
	b.mu.Unlock()
}
