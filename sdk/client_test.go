package sdk

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestConsumeUsesSSE(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/consume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotKey = r.Header.Get("X-RabbiMQ-Key")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("data: hi\n\n"))
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
	if strings.TrimSpace(string(msg)) != "hi" {
		t.Fatalf("unexpected message: %q", string(msg))
	}
	if gotKey != "secret" {
		t.Fatalf("expected key secret, got %q", gotKey)
	}
}
