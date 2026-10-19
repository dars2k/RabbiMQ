package broker

import "sync/atomic"

// Metrics tracks broker activity counters.
type Metrics struct {
	published uint64
	delivered uint64
	rejected  uint64
}

// MetricsSnapshot is a point-in-time view of broker metrics.
type MetricsSnapshot struct {
	Published         uint64 `json:"published"`
	Delivered         uint64 `json:"delivered"`
	Rejected          uint64 `json:"rejected"`
	QueueDepth        int    `json:"queue_depth"`
	HeldDuringShabbat int    `json:"held_during_shabbat"`
	IsShabbat         bool   `json:"is_shabbat"`
	Consumers         int    `json:"consumers"`
	Channels          int    `json:"channels"`
}

// ChannelStats reports pending tasks per channel.
type ChannelStats struct {
	Channel    string `json:"channel"`
	QueueDepth int    `json:"queue_depth"`
}

func (m *Metrics) incPublished() {
	atomic.AddUint64(&m.published, 1)
}

func (m *Metrics) incDelivered() {
	atomic.AddUint64(&m.delivered, 1)
}

func (m *Metrics) incRejected() {
	atomic.AddUint64(&m.rejected, 1)
}

func (m *Metrics) snapshot(queueDepth int, isShabbat bool, consumers, channels int) MetricsSnapshot {
	held := 0
	if isShabbat {
		held = queueDepth
	}

	return MetricsSnapshot{
		Published:         atomic.LoadUint64(&m.published),
		Delivered:         atomic.LoadUint64(&m.delivered),
		Rejected:          atomic.LoadUint64(&m.rejected),
		QueueDepth:        queueDepth,
		HeldDuringShabbat: held,
		IsShabbat:         isShabbat,
		Consumers:         consumers,
		Channels:          channels,
	}
}
