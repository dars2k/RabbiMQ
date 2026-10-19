package http

import (
	"RabbiMQ/api"
	"RabbiMQ/broker"
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockMQ struct {
	mu         sync.Mutex
	publishes  []publishCall
	channels   map[string]chan []byte
	metrics    broker.MetricsSnapshot
	stats      []broker.ChannelStats
	publishErr error
	consumeErr error
	acquireErr error
	isShabbat  bool
	remaining  time.Duration
	consumers  int
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
	if m.publishErr != nil {
		return m.publishErr
	}
	m.publishes = append(m.publishes, publishCall{channel: channel, task: task})
	return nil
}

func (m *mockMQ) channel(name string) chan []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.channels[name]
	if !ok {
		ch = make(chan []byte, 10)
		m.channels[name] = ch
	}
	return ch
}

func (m *mockMQ) Consume(channel string) (<-chan []byte, error) {
	if m.consumeErr != nil {
		return nil, m.consumeErr
	}
	return m.channel(channel), nil
}

func (m *mockMQ) Metrics() broker.MetricsSnapshot {
	return m.metrics
}

func (m *mockMQ) Channels() []broker.ChannelStats {
	return m.stats
}

func (m *mockMQ) ShabbatStatus() (bool, time.Duration) {
	return m.isShabbat, m.remaining
}

func (m *mockMQ) AcquireConsumer() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acquireErr != nil {
		return m.acquireErr
	}
	m.consumers++
	return nil
}

func (m *mockMQ) ReleaseConsumer() {
	m.mu.Lock()
	m.consumers--
	m.mu.Unlock()
}

func (m *mockMQ) consumerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumers
}

func (m *mockMQ) Shutdown() {}

var _ api.RabbiMQ = (*mockMQ)(nil)

func TestLocalServerPublish(t *testing.T) {
	mq := newMockMQ()
	srv := NewLocalServer(mq, "127.0.0.1:0")

	req := httptest.NewRequest(http.MethodPost, "/publish?channel=orders", bytes.NewBufferString("task"))
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)
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

func TestLocalServerPublishErrors(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{broker.ErrShabbatObserved, http.StatusServiceUnavailable, CodeShabbat},
		{broker.ErrQueueFull, http.StatusTooManyRequests, CodeQueueFull},
		{broker.ErrChannelFull, http.StatusTooManyRequests, CodeChannelFull},
		{broker.ErrChannelLimit, http.StatusTooManyRequests, CodeChannelLimit},
		{broker.ErrConsumerLimit, http.StatusTooManyRequests, CodeConsumerLimit},
		{broker.ErrMessageTooLarge, http.StatusRequestEntityTooLarge, CodeMessageTooLarge},
		{broker.ErrInvalidChannel, http.StatusBadRequest, CodeInvalidChannel},
		{broker.ErrClosed, http.StatusServiceUnavailable, CodeUnavailable},
		{errors.New("disk full"), http.StatusInternalServerError, CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.wantCode, func(t *testing.T) {
			mq := newMockMQ()
			mq.publishErr = tc.err
			mq.isShabbat = true
			mq.remaining = 90*time.Minute + 500*time.Millisecond
			srv := NewLocalServer(mq, "")

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader("x")))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get(ErrorHeader); got != tc.wantCode {
				t.Fatalf("error code = %q, want %q", got, tc.wantCode)
			}
			if !strings.Contains(rec.Body.String(), tc.err.Error()) {
				t.Fatalf("body %q does not contain %q", rec.Body.String(), tc.err.Error())
			}
			retry := rec.Header().Get("Retry-After")
			if tc.wantCode == CodeShabbat && retry != "5401" {
				t.Fatalf("Retry-After = %q, want 5401", retry)
			}
			if tc.wantCode != CodeShabbat && retry != "" {
				t.Fatalf("unexpected Retry-After %q", retry)
			}
		})
	}
}

func TestLocalServerPublishBodyLimit(t *testing.T) {
	mq := newMockMQ()
	srv := NewLocalServerWithOptions(mq, Options{MaxBodyBytes: 4})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader("12345")))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader("1234")))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 at the limit", rec.Code)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestLocalServerPublishBadBody(t *testing.T) {
	srv := NewLocalServer(newMockMQ(), "")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/publish", failingReader{}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestLocalServerMethodNotAllowed(t *testing.T) {
	srv := NewLocalServer(newMockMQ(), "")
	tests := map[string]string{
		"/publish":  http.MethodGet,
		"/consume":  http.MethodPost,
		"/metrics":  http.MethodPost,
		"/channels": http.MethodDelete,
		"/healthz":  http.MethodPost,
	}
	for path, method := range tests {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", method, path, rec.Code)
		}
		if rec.Header().Get("Allow") == "" {
			t.Errorf("%s %s: missing Allow header", method, path)
		}
	}
}

func TestLocalServerAuth(t *testing.T) {
	srv := NewLocalServerWithAuth(newMockMQ(), "", "secret")
	for _, path := range []string{"/publish", "/consume", "/metrics", "/channels"} {
		method := http.MethodGet
		if path == "/publish" {
			method = http.MethodPost
		}
		for _, key := range []string{"", "wrong", "secre", "secret!"} {
			req := httptest.NewRequest(method, path, nil)
			if key != "" {
				req.Header.Set(AuthHeader, key)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s with key %q: status = %d, want 401", path, key, rec.Code)
			}
			if rec.Header().Get(ErrorHeader) != CodeUnauthorized {
				t.Errorf("%s: missing error code", path)
			}
		}
	}

	for _, path := range []string{"/metrics", "/channels"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set(AuthHeader, "secret")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s with correct key: status = %d", path, rec.Code)
		}
	}

	// Health checks never need credentials.
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("healthz: %d %q", rec.Code, rec.Body.String())
	}
}

func TestLocalServerMetrics(t *testing.T) {
	mq := newMockMQ()
	mq.metrics = broker.MetricsSnapshot{Published: 2}
	srv := NewLocalServer(mq, "127.0.0.1:0")

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected content type: %q", ct)
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

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/channels", nil))
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

// readEvent reads one SSE event, skipping comment lines, and returns its data.
func readEvent(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var data []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read event: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			if data != nil {
				return strings.Join(data, "\n")
			}
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
		}
	}
}

func openStream(t *testing.T, ctx context.Context, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestLocalServerConsumeStream(t *testing.T) {
	mq := newMockMQ()
	ts := httptest.NewServer(NewLocalServer(mq, "").Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Headers arrive before any task is published.
	resp := openStream(t, ctx, ts.URL+"/consume?channel=orders")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("unexpected content type: %q", ct)
	}

	ch := mq.channel("orders")
	ch <- []byte("hello")
	ch <- []byte("line1\nline2\r\nline3")
	ch <- []byte("  padded  ")

	reader := bufio.NewReader(resp.Body)
	for _, want := range []string{"hello", "line1\nline2\nline3", "  padded  "} {
		if got := readEvent(t, reader); got != want {
			t.Fatalf("event = %q, want %q", got, want)
		}
	}
	if mq.consumerCount() != 1 {
		t.Fatalf("expected 1 consumer, got %d", mq.consumerCount())
	}

	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for mq.consumerCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("consumer slot not released after disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLocalServerConsumeOnce(t *testing.T) {
	mq := newMockMQ()
	ts := httptest.NewServer(NewLocalServer(mq, "").Handler())
	defer ts.Close()

	ch := mq.channel("")
	ch <- []byte("first")
	ch <- []byte("second")

	resp := openStream(t, context.Background(), ts.URL+"/consume?once=true")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "data: first\n\n") || strings.Contains(string(body), "second") {
		t.Fatalf("unexpected once body: %q", body)
	}
	// The second task must still be waiting for the next consumer.
	if got := <-ch; string(got) != "second" {
		t.Fatalf("expected second task to remain, got %q", got)
	}
}

func TestLocalServerConsumeErrors(t *testing.T) {
	t.Run("bad once", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewLocalServer(newMockMQ(), "").Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/consume?once=maybe", nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})
	t.Run("consumer limit", func(t *testing.T) {
		mq := newMockMQ()
		mq.acquireErr = broker.ErrConsumerLimit
		rec := httptest.NewRecorder()
		NewLocalServer(mq, "").Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/consume", nil))
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", rec.Code)
		}
	})
	t.Run("invalid channel", func(t *testing.T) {
		mq := newMockMQ()
		mq.consumeErr = broker.ErrInvalidChannel
		rec := httptest.NewRecorder()
		NewLocalServer(mq, "").Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/consume?channel=a.b", nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if mq.consumerCount() != 0 {
			t.Fatal("consumer slot leaked")
		}
	})
	t.Run("no flusher", func(t *testing.T) {
		w := &plainWriter{header: http.Header{}}
		NewLocalServer(newMockMQ(), "").Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/consume", nil))
		if w.code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", w.code)
		}
	})
}

// plainWriter is a ResponseWriter that does not support flushing.
type plainWriter struct {
	header http.Header
	code   int
}

func (w *plainWriter) Header() http.Header         { return w.header }
func (w *plainWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *plainWriter) WriteHeader(code int)        { w.code = code }

// The stream must end, not spin, when the broker closes the task channel.
func TestLocalServerConsumeEndsOnBrokerShutdown(t *testing.T) {
	mq := newMockMQ()
	ts := httptest.NewServer(NewLocalServer(mq, "").Handler())
	defer ts.Close()

	resp := openStream(t, context.Background(), ts.URL+"/consume")
	close(mq.channel(""))

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not end after broker shutdown")
	}
}

type settableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *settableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *settableClock) set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// End-to-end through the real broker, including Shabbat handling.
func TestLocalServerWithRealBroker(t *testing.T) {
	clock := &settableClock{now: time.Date(2024, 5, 10, 16, 0, 0, 0, time.UTC)} // Friday 4 PM
	b := broker.NewBroker(shabbat.NewShabbatClock(clock), queue.NewInMemoryQueue())
	defer b.Shutdown()
	ts := httptest.NewServer(NewLocalServerWithOptions(b, Options{Passphrase: "k"}).Handler())
	defer ts.Close()

	do := func(method, path, body string) *http.Response {
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set(AuthHeader, "k")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	if resp := do(http.MethodPost, "/publish?channel=bad.name", "x"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid channel status = %d", resp.StatusCode)
	}
	if resp := do(http.MethodPost, "/publish?channel=orders", "payload"); resp.StatusCode != http.StatusOK {
		t.Fatalf("publish status = %d", resp.StatusCode)
	}

	stream := do(http.MethodGet, "/consume?channel=orders&once=1", "")
	if got := readEvent(t, bufio.NewReader(stream.Body)); got != "payload" {
		t.Fatalf("event = %q", got)
	}

	// Friday 6 PM: Shabbat has started. Shabbat ends Saturday 8 PM, 26 hours later.
	clock.set(time.Date(2024, 5, 10, 18, 0, 0, 0, time.UTC))
	resp := do(http.MethodPost, "/publish?channel=orders", "late")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("publish on Shabbat status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "93600" {
		t.Fatalf("Retry-After = %q, want 93600", got)
	}
}

func TestLocalServerListenAndShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := NewLocalServer(newMockMQ(), addr)
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown before start: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	url := fmt.Sprintf("http://%s/healthz", addr)
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("expected ErrServerClosed, got %v", err)
	}
}

func TestFormatEvent(t *testing.T) {
	tests := map[string]string{
		"":          "data: \n\n",
		"a":         "data: a\n\n",
		"a\nb":      "data: a\ndata: b\n\n",
		"a\r\nb\rc": "data: a\ndata: b\ndata: c\n\n",
	}
	for in, want := range tests {
		if got := formatEvent([]byte(in)); got != want {
			t.Errorf("formatEvent(%q) = %q, want %q", in, got, want)
		}
	}
}
