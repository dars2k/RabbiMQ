package broker

import (
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	// ErrShabbatObserved is returned when a client tries to publish a message during Shabbat.
	ErrShabbatObserved = errors.New("cannot publish messages on Shabbat")
	// ErrQueueFull is returned when the total queue limit is reached.
	ErrQueueFull = errors.New("queue limit reached")
	// ErrChannelFull is returned when a channel queue limit is reached.
	ErrChannelFull = errors.New("channel queue limit reached")
	// ErrChannelLimit is returned when the channel count limit is reached.
	ErrChannelLimit = errors.New("too many channels")
	// ErrConsumerLimit is returned when the consumer limit is reached.
	ErrConsumerLimit = errors.New("consumer limit reached")
	// ErrMessageTooLarge is returned when a message exceeds the size limit.
	ErrMessageTooLarge = errors.New("message exceeds size limit")
	// ErrInvalidChannel is returned when a channel name contains unsupported characters.
	ErrInvalidChannel = errors.New("invalid channel name: use up to 64 letters, digits, '-' or '_'")
	// ErrClosed is returned when the broker has been shut down.
	ErrClosed = errors.New("broker is shut down")
)

// MaxChannelNameLength is the longest accepted channel name.
const MaxChannelNameLength = 64

// DefaultChannel is the channel used when no channel name is given.
const DefaultChannel = "default"

// shabbatCheckInterval is how often the broker re-evaluates the Shabbat clock.
const shabbatCheckInterval = time.Second

// Broker is a message broker that observes Shabbat.
//
// Each channel has a FIFO queue and a delivery goroutine. A task is only
// removed from its queue once a consumer has received it, so tasks are never
// dropped by the broker while waiting for a consumer, for Shabbat to end, or
// during shutdown.
type Broker struct {
	shabbatClock *shabbat.ShabbatClock
	mu           sync.Mutex
	isShabbat    bool
	// stateChanged is closed (and replaced) whenever the Shabbat state flips,
	// waking every delivery goroutine.
	stateChanged chan struct{}
	closed       bool
	closeOnce    sync.Once
	wg           sync.WaitGroup
	shutdown     chan struct{}
	metrics      *Metrics
	channels     map[string]*channelState
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
	// depth counts pending tasks, including ones being enqueued. Guarded by Broker.mu.
	depth int
}

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
	config = config.Normalize()
	b := &Broker{
		shabbatClock: shabbatClock,
		isShabbat:    shabbatClock.IsShabbat(),
		stateChanged: make(chan struct{}),
		shutdown:     make(chan struct{}),
		metrics:      &Metrics{},
		channels:     make(map[string]*channelState),
		queueFactory: factory,
		config:       config,
	}

	b.mu.Lock()
	b.addChannelLocked(DefaultChannel, queue)
	b.mu.Unlock()

	b.wg.Add(1)
	go b.shabbatWatcher()

	return b
}

// ValidateChannel reports whether name is an acceptable channel name.
// The empty string selects the default channel.
func ValidateChannel(name string) error {
	if len(name) > MaxChannelNameLength {
		return ErrInvalidChannel
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return ErrInvalidChannel
		}
	}
	return nil
}

func channelName(channel string) string {
	if channel == "" {
		return DefaultChannel
	}
	return channel
}

// Publish adds a task to the broker under a specific channel.
// It returns ErrShabbatObserved if it is currently Shabbat.
func (b *Broker) Publish(channel string, task []byte) error {
	if len(task) > b.config.MaxMessageBytes {
		b.metrics.incRejected()
		return ErrMessageTooLarge
	}

	// Read the clock directly so publishing stops exactly when Shabbat
	// starts rather than on the watcher's next tick.
	if b.checkShabbat() {
		b.metrics.incRejected()
		return ErrShabbatObserved
	}

	state, err := b.getOrCreateChannel(channel)
	if err != nil {
		if !errors.Is(err, ErrClosed) {
			b.metrics.incRejected()
		}
		return err
	}

	// Reserve capacity before enqueueing so concurrent publishers cannot
	// exceed the configured limits.
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.config.MaxQueueDepth > 0 && b.totalDepth >= b.config.MaxQueueDepth {
		b.mu.Unlock()
		b.metrics.incRejected()
		return ErrQueueFull
	}
	if b.config.MaxChannelDepth > 0 && state.depth >= b.config.MaxChannelDepth {
		b.mu.Unlock()
		b.metrics.incRejected()
		return ErrChannelFull
	}
	b.totalDepth++
	state.depth++
	b.mu.Unlock()

	if err := state.queue.Enqueue(task); err != nil {
		b.mu.Lock()
		b.totalDepth--
		state.depth--
		b.mu.Unlock()
		if errors.Is(err, queue.ErrQueueClosed) {
			return ErrClosed
		}
		return err
	}
	b.metrics.incPublished()

	// Non-blocking notify to wake the delivery manager.
	select {
	case state.notify <- struct{}{}:
	default:
	}
	return nil
}

// Consume returns a channel that receives tasks for a specific channel.
// Multiple consumers of the same channel compete for tasks. The returned
// channel is closed when the broker shuts down.
func (b *Broker) Consume(channel string) (<-chan []byte, error) {
	state, err := b.getOrCreateChannel(channel)
	if err != nil {
		closed := make(chan []byte)
		close(closed)
		return closed, err
	}
	return state.taskCh, nil
}

// EnsureChannel opens a channel (and its queue) without publishing to it.
// It is used to restore channels that have tasks persisted from a previous run.
func (b *Broker) EnsureChannel(channel string) error {
	_, err := b.getOrCreateChannel(channel)
	return err
}

// IsShabbat reports whether the broker currently observes Shabbat.
func (b *Broker) IsShabbat() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isShabbat
}

// ShabbatStatus reports whether it is Shabbat and how long until that state next changes.
func (b *Broker) ShabbatStatus() (bool, time.Duration) {
	return b.shabbatClock.Status()
}

// checkShabbat refreshes the Shabbat state from the clock and wakes the
// delivery goroutines if it changed. It returns the current state.
func (b *Broker) checkShabbat() bool {
	isShabbat := b.shabbatClock.IsShabbat()

	b.mu.Lock()
	defer b.mu.Unlock()
	if isShabbat != b.isShabbat {
		b.isShabbat = isShabbat
		close(b.stateChanged)
		b.stateChanged = make(chan struct{})
	}
	return isShabbat
}

// shabbatWatcher monitors for changes in Shabbat state.
func (b *Broker) shabbatWatcher() {
	defer b.wg.Done()
	ticker := time.NewTicker(shabbatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.checkShabbat()
		case <-b.shutdown:
			return
		}
	}
}

// deliveryManager hands tasks from a channel's queue to its consumers in
// FIFO order. It is the only goroutine that removes tasks from that queue,
// so the task returned by Peek is the one later removed by Dequeue.
func (b *Broker) deliveryManager(state *channelState) {
	defer b.wg.Done()
	for {
		b.mu.Lock()
		isShabbat := b.isShabbat
		stateChanged := b.stateChanged
		b.mu.Unlock()

		if isShabbat {
			select {
			case <-stateChanged:
			case <-b.shutdown:
				return
			}
			continue
		}

		task, ok := state.queue.Peek()
		if !ok {
			select {
			case <-state.notify:
			case <-stateChanged:
			case <-b.shutdown:
				return
			}
			continue
		}

		select {
		case state.taskCh <- task:
			state.queue.Dequeue()
			b.mu.Lock()
			b.totalDepth--
			state.depth--
			b.mu.Unlock()
			b.metrics.incDelivered()
		case <-stateChanged:
			// Shabbat may have started; re-check before delivering.
		case <-b.shutdown:
			return
		}
	}
}

// Shutdown gracefully shuts down the broker. Pending tasks remain in their
// queues. It is safe to call more than once.
func (b *Broker) Shutdown() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		states := b.statesLocked()
		b.mu.Unlock()

		close(b.shutdown)
		b.wg.Wait()

		for _, state := range states {
			close(state.taskCh)
			state.queue.Close()
		}
	})
}

// Metrics returns a snapshot of broker metrics.
func (b *Broker) Metrics() MetricsSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.metrics.snapshot(b.totalDepth, b.isShabbat, b.consumers, len(b.channels))
}

// Channels returns per-channel queue depth information, sorted by name.
func (b *Broker) Channels() []ChannelStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	stats := make([]ChannelStats, 0, len(b.channels))
	for _, state := range b.channels {
		stats = append(stats, ChannelStats{
			Channel:    state.channel,
			QueueDepth: state.depth,
		})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Channel < stats[j].Channel })
	return stats
}

func (b *Broker) statesLocked() []*channelState {
	states := make([]*channelState, 0, len(b.channels))
	for _, state := range b.channels {
		states = append(states, state)
	}
	return states
}

func (b *Broker) getOrCreateChannel(channel string) (*channelState, error) {
	if err := ValidateChannel(channel); err != nil {
		return nil, err
	}
	name := channelName(channel)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	if state, ok := b.channels[name]; ok {
		b.mu.Unlock()
		return state, nil
	}
	if b.config.MaxChannels > 0 && len(b.channels) >= b.config.MaxChannels {
		b.mu.Unlock()
		return nil, ErrChannelLimit
	}
	b.mu.Unlock()

	// Create the queue outside the lock: it may touch the filesystem.
	var q queue.Queue
	if b.queueFactory != nil {
		var err error
		q, err = b.queueFactory(name)
		if err != nil {
			return nil, err
		}
	} else {
		q = queue.NewInMemoryQueue()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.channels[name]; ok {
		// Another goroutine created the channel first.
		q.Close()
		return existing, nil
	}
	if b.closed {
		q.Close()
		return nil, ErrClosed
	}
	if b.config.MaxChannels > 0 && len(b.channels) >= b.config.MaxChannels {
		q.Close()
		return nil, ErrChannelLimit
	}
	return b.addChannelLocked(name, q), nil
}

// addChannelLocked registers a channel and starts its delivery goroutine.
// b.mu must be held.
func (b *Broker) addChannelLocked(name string, q queue.Queue) *channelState {
	state := &channelState{
		queue:   q,
		taskCh:  make(chan []byte),
		notify:  make(chan struct{}, 1),
		channel: name,
		depth:   q.Size(),
	}
	b.channels[name] = state
	b.totalDepth += state.depth
	b.wg.Add(1)
	go b.deliveryManager(state)
	return state
}

// AcquireConsumer reserves a consumer slot if available.
func (b *Broker) AcquireConsumer() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
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
