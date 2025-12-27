package api

import (
	"RabbiMQ/broker"
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"path/filepath"
	"strings"
)

// RabbiMQ is the interface for the RabbiMQ message broker.
type RabbiMQ interface {
	// Publish adds a task to the broker.
	Publish(channel string, task []byte) error
	// Consume returns a channel that receives tasks for a specific channel.
	Consume(channel string) (<-chan []byte, error)
	// Metrics returns a snapshot of broker metrics.
	Metrics() broker.MetricsSnapshot
	// Channels returns per-channel queue depths.
	Channels() []broker.ChannelStats
	// AcquireConsumer reserves a consumer slot if available.
	AcquireConsumer() error
	// ReleaseConsumer releases a previously acquired consumer slot.
	ReleaseConsumer()
	// Shutdown gracefully shuts down the broker.
	Shutdown()
}

// rabbimq is a concrete implementation of the RabbiMQ interface.
type rabbimq struct {
	broker *broker.Broker
}

// New creates a new RabbiMQ instance with an in-memory queue.
func New() RabbiMQ {
	return NewWithConfig(Config{})
}

// NewWithConfig creates a new RabbiMQ instance with an in-memory queue and config.
func NewWithConfig(cfg Config) RabbiMQ {
	return newWithClock(&shabbat.RealClock{}, cfg)
}

func newWithClock(clock shabbat.Clock, cfg Config) RabbiMQ {
	shabbatClock := shabbat.NewShabbatClock(clock)
	q := queue.NewInMemoryQueue()
	b := broker.NewBrokerWithFactoryConfig(shabbatClock, q, func(channel string) (queue.Queue, error) {
		return queue.NewInMemoryQueue(), nil
	}, cfg.Limits)
	return &rabbimq{broker: b}
}

// NewWithFile creates a new RabbiMQ instance with a persistent queue.
func NewWithFile(filePath string) (RabbiMQ, error) {
	return NewWithFileConfig(filePath, Config{})
}

// NewWithFileConfig creates a new RabbiMQ instance with a persistent queue and config.
func NewWithFileConfig(filePath string, cfg Config) (RabbiMQ, error) {
	shabbatClock := shabbat.NewShabbatClock(&shabbat.RealClock{})
	q, err := queue.NewPersistentQueue(filePath)
	if err != nil {
		return nil, err
	}
	b := broker.NewBrokerWithFactoryConfig(shabbatClock, q, func(channel string) (queue.Queue, error) {
		path := channelLogPath(filePath, channel)
		return queue.NewPersistentQueue(path)
	}, cfg.Limits)
	return &rabbimq{broker: b}, nil
}

func channelLogPath(basePath, channel string) string {
	dir := filepath.Dir(basePath)
	base := filepath.Base(basePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if ext == "" {
		ext = ".log"
	}
	if name == "" {
		name = "rabbimq"
	}
	return filepath.Join(dir, name+"."+sanitizeChannel(channel)+ext)
}

func sanitizeChannel(channel string) string {
	if channel == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range channel {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

// Publish adds a task to the broker.
// Publish adds a task to a specific channel.
func (r *rabbimq) Publish(channel string, task []byte) error {
	return r.broker.Publish(channel, task)
}

// Consume returns a channel that receives tasks for a specific channel.
func (r *rabbimq) Consume(channel string) (<-chan []byte, error) {
	return r.broker.Consume(channel)
}

// Metrics returns a snapshot of broker metrics.
func (r *rabbimq) Metrics() broker.MetricsSnapshot {
	return r.broker.Metrics()
}

// Channels returns per-channel queue depths.
func (r *rabbimq) Channels() []broker.ChannelStats {
	return r.broker.Channels()
}

// AcquireConsumer reserves a consumer slot if available.
func (r *rabbimq) AcquireConsumer() error {
	return r.broker.AcquireConsumer()
}

// ReleaseConsumer releases a previously acquired consumer slot.
func (r *rabbimq) ReleaseConsumer() {
	r.broker.ReleaseConsumer()
}

// Shutdown gracefully shuts down the broker.
func (r *rabbimq) Shutdown() {
	r.broker.Shutdown()
}
