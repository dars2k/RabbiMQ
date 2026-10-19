package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type result struct {
	code   int
	stdout string
	stderr string
}

func runCLI(ctx context.Context, stdin string, args ...string) result {
	var stdout, stderr bytes.Buffer
	code := run(ctx, args, strings.NewReader(stdin), &stdout, &stderr)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

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

	res := runCLI(context.Background(), "", "publish", "-addr", srv.URL, "-channel", "orders", "hello", "world")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if gotPath != "/publish?channel=orders" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotBody != "hello world" {
		t.Fatalf("unexpected body: %q", gotBody)
	}
}

func TestRunPublishFromStdinAndConfig(t *testing.T) {
	var gotKey, gotBody, gotChannel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-RabbiMQ-Key")
		gotChannel = r.URL.Query().Get("channel")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
	}))
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "client.yaml")
	cfg := fmt.Sprintf("address: %s\npassphrase: from-config\nchannel: cfg-channel\n", srv.URL)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0644); err != nil {
		t.Fatal(err)
	}

	res := runCLI(context.Background(), "  piped task\n", "publish", "-config", cfgPath)
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if gotKey != "from-config" || gotBody != "piped task" || gotChannel != "cfg-channel" {
		t.Fatalf("unexpected request: key=%q body=%q channel=%q", gotKey, gotBody, gotChannel)
	}

	// Explicit flags win over the config file.
	res = runCLI(context.Background(), "", "publish", "-config", cfgPath, "-passphrase", "flag", "-channel", "flag-channel", "x")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if gotKey != "flag" || gotChannel != "flag-channel" {
		t.Fatalf("flags should override config: key=%q channel=%q", gotKey, gotChannel)
	}
}

func TestRunPublishErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RabbiMQ-Error", "shabbat")
		http.Error(w, "cannot publish messages on Shabbat", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	ctx := context.Background()

	res := runCLI(ctx, "", "publish", "-addr", srv.URL, "task")
	if res.code != 1 || !strings.Contains(res.stderr, "Shabbat") {
		t.Fatalf("expected Shabbat failure, got %d %q", res.code, res.stderr)
	}

	res = runCLI(ctx, "   ", "publish", "-addr", srv.URL)
	if res.code != 2 || !strings.Contains(res.stderr, "no task provided") {
		t.Fatalf("expected usage error for empty task, got %d %q", res.code, res.stderr)
	}

	res = runCLI(ctx, "", "publish", "-addr", "not a url", "x")
	if res.code != 2 {
		t.Fatalf("expected usage error for bad address, got %d %q", res.code, res.stderr)
	}

	res = runCLI(ctx, "", "publish", "-config", "/does/not/exist.yaml", "x")
	if res.code != 1 || !strings.Contains(res.stderr, "config error") {
		t.Fatalf("expected config error, got %d %q", res.code, res.stderr)
	}

	res = runCLI(ctx, "", "publish", "-bogus")
	if res.code != 2 {
		t.Fatalf("expected usage error for unknown flag, got %d", res.code)
	}
}

func TestRunChannelIsEscaped(t *testing.T) {
	var rawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
	}))
	defer srv.Close()

	runCLI(context.Background(), "", "publish", "-addr", srv.URL, "-channel", "a&b=c", "x")
	if rawQuery != "channel=a%26b%3Dc" {
		t.Fatalf("channel not escaped: %q", rawQuery)
	}
}

func sseServer(events ...string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/consume" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for _, e := range events {
			_, _ = io.WriteString(w, "data: "+e+"\n\n")
		}
		w.(http.Flusher).Flush()
	}))
}

func TestRunConsumeOnce(t *testing.T) {
	srv := sseServer("hi")
	defer srv.Close()

	res := runCLI(context.Background(), "", "consume", "-addr", srv.URL, "-once")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if res.stdout != "hi\n" {
		t.Fatalf("unexpected output: %q", res.stdout)
	}
}

func TestRunConsumeStream(t *testing.T) {
	srv := sseServer("one", "two")
	defer srv.Close()

	// The test server ends the stream after two events, which the CLI reports.
	res := runCLI(context.Background(), "", "consume", "-addr", srv.URL)
	if res.stdout != "one\ntwo\n" {
		t.Fatalf("unexpected output: %q", res.stdout)
	}
	if res.code != 1 || !strings.Contains(res.stderr, "stream closed") {
		t.Fatalf("expected stream closed error, got %d %q", res.code, res.stderr)
	}
}

func TestRunConsumeCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	for _, extra := range [][]string{nil, {"-once"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		args := append([]string{"consume", "-addr", srv.URL}, extra...)
		res := runCLI(ctx, "", args...)
		cancel()
		if res.code != 0 {
			t.Fatalf("%v: interrupted consume should exit cleanly, got %d %q", extra, res.code, res.stderr)
		}
	}
}

func TestRunMetricsAndChannels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			_, _ = io.WriteString(w, `{"published":4,"is_shabbat":true}`)
		case "/channels":
			_, _ = io.WriteString(w, `[{"channel":"orders","queue_depth":2}]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	res := runCLI(context.Background(), "", "metrics", "-addr", srv.URL)
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(res.stdout), &m); err != nil || m["published"] != float64(4) || m["is_shabbat"] != true {
		t.Fatalf("unexpected metrics output %q: %v", res.stdout, err)
	}

	res = runCLI(context.Background(), "", "channels", "-addr", srv.URL)
	if res.code != 0 || !strings.Contains(res.stdout, `"channel": "orders"`) {
		t.Fatalf("unexpected channels output: %d %q %q", res.code, res.stdout, res.stderr)
	}

	srv.Close()
	if res := runCLI(context.Background(), "", "metrics", "-addr", srv.URL); res.code != 1 {
		t.Fatalf("expected failure against closed server, got %d", res.code)
	}
	if res := runCLI(context.Background(), "", "channels", "-addr", srv.URL); res.code != 1 {
		t.Fatalf("expected failure against closed server, got %d", res.code)
	}
}

func TestRunUsage(t *testing.T) {
	ctx := context.Background()
	if res := runCLI(ctx, "", "frobnicate"); res.code != 2 || !strings.Contains(res.stderr, "unknown command") {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res := runCLI(ctx, "", "help"); res.code != 0 || !strings.Contains(res.stdout, "Usage:") {
		t.Fatalf("unexpected help: %+v", res)
	}
	if res := runCLI(ctx, "", "publish", "-h"); res.code != 0 {
		t.Fatalf("-h should exit 0, got %d", res.code)
	}
	if res := runCLI(ctx, "", "-tls-cert", "only.crt"); res.code != 2 || !strings.Contains(res.stderr, "-tls-key") {
		t.Fatalf("expected TLS usage error, got %+v", res)
	}
	if res := runCLI(ctx, "", "serve", "extra"); res.code != 2 {
		t.Fatalf("expected usage error for extra args, got %+v", res)
	}
	if res := runCLI(ctx, "", "-config", "/does/not/exist.yaml"); res.code != 1 || !strings.Contains(res.stderr, "load config") {
		t.Fatalf("expected config error, got %+v", res)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startServer runs the broker in-process and returns a stop function that
// simulates SIGTERM and waits for a clean exit.
func startServer(t *testing.T, args ...string) (addr string, stop func()) {
	t.Helper()
	addr = freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	stderr := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, append([]string{"-addr", addr}, args...), strings.NewReader(""), io.Discard, stderr)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		select {
		case code := <-done:
			t.Fatalf("server exited early with %d: %s", code, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return addr, func() {
		cancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Fatalf("server exited with %d: %s", code, stderr.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server did not shut down")
		}
	}
}

func TestServerLifecycle(t *testing.T) {
	if time.Now().Weekday() == time.Friday || time.Now().Weekday() == time.Saturday {
		// The real server uses the wall clock; publishing may be blocked.
		t.Skip("skipping wall-clock server test near Shabbat")
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "server.yaml")
	dataFile := filepath.Join(dir, "queue.log")
	cfg := fmt.Sprintf("passphrase: s3cret\ndataFile: %s\nlimits:\n  maxChannelDepth: 5\n", dataFile)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0644); err != nil {
		t.Fatal(err)
	}

	addr, stop := startServer(t, "-config", cfgPath)
	client := []string{"-addr", "http://" + addr, "-passphrase", "s3cret"}
	ctx := context.Background()

	for _, msg := range []string{"first", "second"} {
		res := runCLI(ctx, "", append(append([]string{"publish"}, client...), "-channel", "orders", msg)...)
		if res.code != 0 {
			t.Fatalf("publish exit %d: %s", res.code, res.stderr)
		}
	}
	res := runCLI(ctx, "", append(append([]string{"consume"}, client...), "-channel", "orders", "-once")...)
	if res.code != 0 || res.stdout != "first\n" {
		t.Fatalf("consume: %d %q %q", res.code, res.stdout, res.stderr)
	}
	if res := runCLI(ctx, "", append([]string{"metrics"}, "-addr", "http://"+addr)...); res.code != 1 {
		t.Fatalf("metrics without passphrase should be rejected, got %d", res.code)
	}
	stop()

	if _, err := os.Stat(filepath.Join(dir, "queue.orders.log")); err != nil {
		t.Fatalf("expected channel log next to data file: %v", err)
	}

	// After a restart only the unconsumed message remains.
	addr, stop = startServer(t, "-config", cfgPath)
	defer stop()
	client = []string{"-addr", "http://" + addr, "-passphrase", "s3cret"}
	res = runCLI(ctx, "", append([]string{"channels"}, client...)...)
	if res.code != 0 || !strings.Contains(res.stdout, `"channel": "orders",`+"\n"+`    "queue_depth": 1`) {
		t.Fatalf("unexpected channels after restart: %d %q %q", res.code, res.stdout, res.stderr)
	}
	res = runCLI(ctx, "", append(append([]string{"consume"}, client...), "-channel", "orders", "-once")...)
	if res.stdout != "second\n" {
		t.Fatalf("expected second after restart, got %q %q", res.stdout, res.stderr)
	}
}

func TestServerListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	dataFile := filepath.Join(t.TempDir(), "q.log")
	res := runCLI(context.Background(), "", "-addr", ln.Addr().String(), "-data", dataFile)
	if res.code != 1 || !strings.Contains(res.stderr, "address already in use") {
		t.Fatalf("expected listen failure, got %+v", res)
	}
}

func TestServerBadDataFile(t *testing.T) {
	res := runCLI(context.Background(), "", "-addr", freeAddr(t), "-data", t.TempDir())
	if res.code != 1 || !strings.Contains(res.stderr, "create broker") {
		t.Fatalf("expected broker creation failure, got %+v", res)
	}
}
