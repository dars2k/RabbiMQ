package sdk

import (
	"RabbiMQ/broker"
	rmqhttp "RabbiMQ/http"
	"RabbiMQ/queue"
	"RabbiMQ/shabbat"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPublishSendsChannel(t *testing.T) {
	var gotChannel string
	var gotBody string
	var gotKey string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/publish" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotChannel = r.URL.Query().Get("channel")
		gotKey = r.Header.Get("X-RabbiMQ-Key")
		data, _ := io.ReadAll(r.Body)
		gotBody = string(data)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := NewClient(Config{Address: srv.URL, Passphrase: "secret"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Publish(ctx, "orders", []byte("hello")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if gotChannel != "orders" {
		t.Fatalf("expected channel orders, got %q", gotChannel)
	}
	if gotBody != "hello" {
		t.Fatalf("expected body hello, got %q", gotBody)
	}
	if gotKey != "secret" {
		t.Fatalf("expected key secret, got %q", gotKey)
	}
}

func TestDefaultChannelFromConfig(t *testing.T) {
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.URL.Query().Get("channel"))
		mu.Unlock()
	}))
	defer srv.Close()

	client, err := NewClient(Config{Address: srv.URL, Channel: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = client.Publish(ctx, "", nil)
	_ = client.Publish(ctx, "override", nil)

	mu.Lock()
	defer mu.Unlock()
	if strings.Join(got, ",") != "orders,override" {
		t.Fatalf("unexpected channels: %v", got)
	}
}

func TestConsumeUsesSSE(t *testing.T) {
	var gotKey, gotOnce string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/consume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotKey = r.Header.Get("X-RabbiMQ-Key")
		gotOnce = r.URL.Query().Get("once")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(": connected\n\ndata: hi\n\n"))
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	client, err := NewClient(Config{Address: srv.URL, Passphrase: "secret"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, ok, err := client.Consume(ctx, "orders")
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	if !ok {
		t.Fatal("expected message")
	}
	if string(msg) != "hi" {
		t.Fatalf("unexpected message: %q", string(msg))
	}
	if gotKey != "secret" {
		t.Fatalf("expected key secret, got %q", gotKey)
	}
	if gotOnce != "true" {
		t.Fatalf("Consume should request a single message, got once=%q", gotOnce)
	}
}

func TestConsumeStreamClosedWithoutMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": connected\n\n"))
	}))
	defer srv.Close()

	client, _ := NewClient(Config{Address: srv.URL})
	msg, ok, err := client.Consume(context.Background(), "")
	if err != nil || ok || msg != nil {
		t.Fatalf("expected clean empty result, got %q %v %v", msg, ok, err)
	}
}

func TestConsumeContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	client, _ := NewClient(Config{Address: srv.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := client.Consume(ctx, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestAPIErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RabbiMQ-Error", "shabbat")
		w.Header().Set("Retry-After", "120")
		http.Error(w, "cannot publish messages on Shabbat", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client, _ := NewClient(Config{Address: srv.URL})
	ctx := context.Background()

	err := client.Publish(ctx, "", []byte("x"))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T %v", err, err)
	}
	if apiErr.StatusCode != http.StatusServiceUnavailable || apiErr.Code != CodeShabbat || apiErr.RetryAfter != 2*time.Minute {
		t.Fatalf("unexpected APIError: %+v", apiErr)
	}
	if !IsShabbat(err) || !apiErr.Temporary() {
		t.Fatal("expected a temporary Shabbat error")
	}
	if !strings.Contains(err.Error(), "Shabbat") || !strings.Contains(err.Error(), "503") {
		t.Fatalf("unexpected message: %v", err)
	}

	if _, _, err := client.Consume(ctx, ""); !IsShabbat(err) {
		t.Fatalf("Consume should surface APIError, got %v", err)
	}
	msgs, errs := client.Subscribe(ctx, "")
	if err := <-errs; !IsShabbat(err) {
		t.Fatalf("Subscribe should surface APIError, got %v", err)
	}
	if _, ok := <-msgs; ok {
		t.Fatal("expected closed message channel")
	}
	if _, err := client.Metrics(ctx); !errors.As(err, &apiErr) {
		t.Fatalf("Metrics should surface APIError, got %v", err)
	}
	if err := client.Healthy(ctx); !errors.As(err, &apiErr) {
		t.Fatalf("Healthy should surface APIError, got %v", err)
	}
}

func TestAPIErrorFormatting(t *testing.T) {
	err := &APIError{StatusCode: http.StatusBadRequest}
	if err.Error() != "rabbimq: Bad Request (HTTP 400)" {
		t.Fatalf("unexpected message: %q", err.Error())
	}
	if err.Temporary() {
		t.Fatal("400 should not be temporary")
	}
	if !(&APIError{StatusCode: http.StatusTooManyRequests}).Temporary() {
		t.Fatal("429 should be temporary")
	}
	if IsShabbat(errors.New("other")) {
		t.Fatal("plain error is not Shabbat")
	}
}

func TestNewClientValidation(t *testing.T) {
	bad := []Config{
		{},
		{Address: "127.0.0.1:8080"},
		{Address: "ftp://host"},
		{Address: "http://"},
		{Address: "http://host", Timeout: "soon"},
		{Address: "http://host", Timeout: "-1s"},
		{Address: "http://host", CACertFile: "/does/not/exist.pem"},
	}
	for _, cfg := range bad {
		if _, err := NewClient(cfg); err == nil {
			t.Errorf("expected error for %+v", cfg)
		}
	}

	badPEM := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(badPEM, []byte("not a cert"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(Config{Address: "https://host", CACertFile: badPEM}); err == nil {
		t.Fatal("expected error for unparsable CA file")
	}
	if _, err := NewClient(Config{Address: "https://host", InsecureSkipVerify: true}); err != nil {
		t.Fatalf("insecure config should be valid: %v", err)
	}
}

func TestNewClientWithCACert(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	}))
	defer srv.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, certPEM(srv.Certificate().Raw), 0644); err != nil {
		t.Fatal(err)
	}

	client, err := NewClient(Config{Address: srv.URL, CACertFile: caPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Healthy(context.Background()); err != nil {
		t.Fatalf("TLS request with CA file failed: %v", err)
	}
}

// --- End-to-end tests against the real server and broker ---

type e2e struct {
	client *Client
	broker *broker.Broker
}

func newE2E(t *testing.T, cfg broker.Config) e2e {
	t.Helper()
	monday := time.Date(2024, 5, 13, 10, 0, 0, 0, time.UTC)
	b := broker.NewBrokerWithFactoryConfig(
		shabbat.NewShabbatClock(&shabbat.FixedClock{Time: monday}),
		queue.NewInMemoryQueue(), nil, cfg)
	t.Cleanup(b.Shutdown)

	ts := httptest.NewServer(rmqhttp.NewLocalServerWithAuth(b, "", "secret").Handler())
	t.Cleanup(ts.Close)

	client, err := NewClient(Config{Address: ts.URL + "/", Passphrase: "secret", Timeout: "2s"})
	if err != nil {
		t.Fatal(err)
	}
	return e2e{client: client, broker: b}
}

func TestEndToEndPublishConsume(t *testing.T) {
	env := newE2E(t, broker.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payloads := []string{"first", "multi\nline", " spaced "}
	for _, p := range payloads {
		if err := env.client.Publish(ctx, "orders", []byte(p)); err != nil {
			t.Fatal(err)
		}
	}

	// Consume must take exactly one message per call and lose nothing.
	for _, want := range payloads {
		msg, ok, err := env.client.Consume(ctx, "orders")
		if err != nil || !ok {
			t.Fatalf("consume: %v %v", ok, err)
		}
		if string(msg) != want {
			t.Fatalf("got %q, want %q", msg, want)
		}
	}

	m, err := env.client.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.Published != 3 || m.Delivered != 3 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
	stats, err := env.client.Channels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || stats[1].Channel != "orders" {
		t.Fatalf("unexpected channels: %+v", stats)
	}
	if err := env.client.Healthy(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEndToEndSubscribe(t *testing.T) {
	env := newE2E(t, broker.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subCtx, stop := context.WithCancel(ctx)
	msgs, errs := env.client.Subscribe(subCtx, "events")

	const n = 20
	go func() {
		for i := 0; i < n; i++ {
			if err := env.client.Publish(ctx, "events", []byte(fmt.Sprintf("e%d", i))); err != nil {
				t.Errorf("publish: %v", err)
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		select {
		case msg := <-msgs:
			if want := fmt.Sprintf("e%d", i); string(msg) != want {
				t.Fatalf("got %q, want %q", msg, want)
			}
		case err := <-errs:
			t.Fatalf("subscription failed: %v", err)
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}

	stop()
	for range msgs {
	}
	if err, ok := <-errs; ok && err != nil {
		t.Fatalf("cancel should not report an error, got %v", err)
	}
}

func TestEndToEndSubscribeServerShutdown(t *testing.T) {
	env := newE2E(t, broker.Config{})
	msgs, errs := env.client.Subscribe(context.Background(), "")

	// Wait until the consumer is registered, then stop the broker.
	deadline := time.Now().Add(2 * time.Second)
	for env.broker.Metrics().Consumers == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber never connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	env.broker.Shutdown()

	select {
	case err := <-errs:
		if !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("expected ErrStreamClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not end")
	}
	if _, ok := <-msgs; ok {
		t.Fatal("expected closed message channel")
	}
}

func TestEndToEndErrors(t *testing.T) {
	env := newE2E(t, broker.Config{MaxChannelDepth: 1, MaxMessageBytes: 8})
	ctx := context.Background()

	if err := env.client.Publish(ctx, "c", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		channel, payload, code string
	}{
		{"c", "full", CodeChannelFull},
		{"c", "much too large", CodeMessageTooLarge},
		{"bad.name", "x", CodeInvalidChannel},
	}
	for _, tc := range tests {
		err := env.client.Publish(ctx, tc.channel, []byte(tc.payload))
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != tc.code {
			t.Errorf("publish %q to %q: got %v, want code %s", tc.payload, tc.channel, err, tc.code)
		}
	}

	wrongKey, _ := NewClient(Config{Address: env.client.baseURL, Passphrase: "nope"})
	var apiErr *APIError
	if err := wrongKey.Publish(ctx, "", nil); !errors.As(err, &apiErr) || apiErr.Code != CodeUnauthorized {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}
