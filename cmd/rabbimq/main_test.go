package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunPublish(t *testing.T) {
	var gotPath string
	var gotBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path + "?" + r.URL.RawQuery
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	runPublish([]string{"-addr", srv.URL, "-channel", "orders", "hello"})

	if gotPath != "/publish?channel=orders" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotBody != "hello" {
		t.Fatalf("unexpected body: %q", gotBody)
	}
}

func TestRunConsumeOnce(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/consume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("data: hi\n\n"))
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	runConsume([]string{"-addr", srv.URL, "-once"})
}
