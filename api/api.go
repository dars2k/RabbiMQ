package api

import (
	"RabbiMQ/broker"
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
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
	// ShabbatStatus reports whether it is Shabbat and how long until that state next changes.
	ShabbatStatus() (bool, time.Duration)
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
// The default channel is stored at filePath and every other channel in a
// sibling file (for "rabbimq.log" and channel "orders": "rabbimq.orders.log").
// Channels persisted by a previous run are reopened so their pending tasks
// are delivered and reported.
func NewWithFileConfig(filePath string, cfg Config) (RabbiMQ, error) {
	return newWithFileClock(&shabbat.RealClock{}, filePath, cfg)
}

func newWithFileClock(clock shabbat.Clock, filePath string, cfg Config) (RabbiMQ, error) {
	shabbatClock := shabbat.NewShabbatClock(clock)
	q, err := queue.NewPersistentQueue(filePath)
	if err != nil {
		return nil, err
	}
	b := broker.NewBrokerWithFactoryConfig(shabbatClock, q, func(channel string) (queue.Queue, error) {
		return queue.NewPersistentQueue(channelLogPath(filePath, channel))
	}, cfg.Limits)

	channels, err := persistedChannels(filePath)
	if err != nil {
		b.Shutdown()
		return nil, err
	}
	for _, channel := range channels {
		if err := b.EnsureChannel(channel); err != nil {
			b.Shutdown()
			return nil, fmt.Errorf("restore channel %q: %w", channel, err)
		}
	}
	return &rabbimq{broker: b}, nil
}

// splitLogPath splits a log path into its directory, file stem and extension.
func splitLogPath(basePath string) (dir, name, ext string) {
	dir = filepath.Dir(basePath)
	base := filepath.Base(basePath)
	ext = filepath.Ext(base)
	name = strings.TrimSuffix(base, ext)
	if ext == "" {
		ext = ".log"
	}
	if name == "" {
		name = "rabbimq"
	}
	return dir, name, ext
}

// channelLogPath returns the log file for a channel. Channel names are
// validated by the broker, so they are safe to use in file names.
func channelLogPath(basePath, channel string) string {
	dir, name, ext := splitLogPath(basePath)
	return filepath.Join(dir, name+"."+channel+ext)
}

// persistedChannels lists the channels that have a log file next to basePath.
func persistedChannels(basePath string) ([]string, error) {
	dir, name, ext := splitLogPath(basePath)
	prefix := name + "."
	matches, err := filepath.Glob(filepath.Join(dir, globEscape(prefix)+"*"+globEscape(ext)))
	if err != nil {
		return nil, err
	}

	var channels []string
	for _, match := range matches {
		channel := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(match), prefix), ext)
		if channel == "" || channel == broker.DefaultChannel || broker.ValidateChannel(channel) != nil {
			continue
		}
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels, nil
}

func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

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

// ShabbatStatus reports whether it is Shabbat and how long until that state next changes.
func (r *rabbimq) ShabbatStatus() (bool, time.Duration) {
	return r.broker.ShabbatStatus()
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
