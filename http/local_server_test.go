package http

import (
	"RabbiMQ/api"
	"RabbiMQ/broker"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type mockMQ struct {
	mu        sync.Mutex
	publishes []publishCall
	channels  map[string]chan []byte
	metrics   broker.MetricsSnapshot
	stats     []broker.ChannelStats
}

type publishCall struct {
	channel string
	task    []byte
}

func newMockMQ() *mockMQ {
	return &mockMQ{channels: make(map[string]chan []byte)}
}

func (m *mockMQ) Publish(channel string, task []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishes = append(m.publishes, publishCall{channel: channel, task: task})
	return nil
}

func (m *mockMQ) Consume(channel string) (<-chan []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.channels[channel]
	if !ok {
		ch = make(chan []byte, 1)
		m.channels[channel] = ch
	}
	return ch, nil
}

func (m *mockMQ) Metrics() broker.MetricsSnapshot {
	return m.metrics
}

func (m *mockMQ) Channels() []broker.ChannelStats {
	return m.stats
}

func (m *mockMQ) AcquireConsumer() error {
	return nil
}

func (m *mockMQ) ReleaseConsumer() {}

func (m *mockMQ) Shutdown() {}

var _ api.RabbiMQ = (*mockMQ)(nil)

type streamRecorder struct {
	headers http.Header
	buf     bytes.Buffer
	code    int
}

func (s *streamRecorder) Header() http.Header {
	if s.headers == nil {
		s.headers = make(http.Header)
	}
	return s.headers
}

func (s *streamRecorder) Write(p []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.buf.Write(p)
}

func (s *streamRecorder) WriteHeader(code int) {
	s.code = code
}

func (s *streamRecorder) Flush() {}

func TestLocalServerPublish(t *testing.T) {
	mq := newMockMQ()
	srv := NewLocalServer(mq, "127.0.0.1:0")

	req := httptest.NewRequest(http.MethodPost, "/publish?channel=orders", bytes.NewBufferString("task"))
	rec := httptest.NewRecorder()

	srv.handlePublish(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}

	mq.mu.Lock()
	defer mq.mu.Unlock()
	if len(mq.publishes) != 1 {
		t.Fatalf("expected publish call")
	}
	if mq.publishes[0].channel != "orders" {
		t.Fatalf("expected channel orders, got %q", mq.publishes[0].channel)
	}
	if string(mq.publishes[0].task) != "task" {
		t.Fatalf("unexpected task: %q", string(mq.publishes[0].task))
	}
}

func TestLocalServerMetrics(t *testing.T) {
	mq := newMockMQ()
	mq.metrics = broker.MetricsSnapshot{Published: 2}
	srv := NewLocalServer(mq, "127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	srv.handleMetrics(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}

	var got broker.MetricsSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if got.Published != 2 {
		t.Fatalf("unexpected metrics: %+v", got)
	}
}

func TestLocalServerChannels(t *testing.T) {
	mq := newMockMQ()
	mq.stats = []broker.ChannelStats{{Channel: "orders", QueueDepth: 3}}
	srv := NewLocalServer(mq, "127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/channels", nil)
	rec := httptest.NewRecorder()

	srv.handleChannels(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}

	var got []broker.ChannelStats
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode channels: %v", err)
	}
	if len(got) != 1 || got[0].Channel != "orders" {
		t.Fatalf("unexpected channels: %+v", got)
	}
}

func TestLocalServerConsumeStream(t *testing.T) {
	mq := newMockMQ()
	srv := NewLocalServer(mq, "127.0.0.1:0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/consume?channel=orders", nil).WithContext(ctx)
	rec := &streamRecorder{}

	done := make(chan struct{})
	go func() {
		srv.handleConsume(rec, req)
		close(done)
	}()

	_, _ = mq.Consume("orders")
	mq.mu.Lock()
	ch := mq.channels["orders"]
	mq.mu.Unlock()
	ch <- []byte("hello")

	deadline := time.After(2 * time.Second)
	for {
		if bytes.Contains(rec.buf.Bytes(), []byte("data: hello")) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for stream data")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}
