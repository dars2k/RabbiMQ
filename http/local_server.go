package http

import (
	"RabbiMQ/api"
	"RabbiMQ/broker"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AuthHeader carries the shared passphrase on authenticated requests.
const AuthHeader = "X-RabbiMQ-Key"

// ErrorHeader carries a stable, machine-readable error code on failed requests.
const ErrorHeader = "X-RabbiMQ-Error"

// Error codes sent in ErrorHeader.
const (
	CodeShabbat         = "shabbat"
	CodeQueueFull       = "queue_full"
	CodeChannelFull     = "channel_full"
	CodeChannelLimit    = "channel_limit"
	CodeConsumerLimit   = "consumer_limit"
	CodeMessageTooLarge = "message_too_large"
	CodeInvalidChannel  = "invalid_channel"
	CodeUnavailable     = "unavailable"
	CodeUnauthorized    = "unauthorized"
	CodeInternal        = "internal"
)

// keepAliveInterval is how often an idle SSE stream sends a comment line.
const keepAliveInterval = 15 * time.Second

// Options configures a LocalServer.
type Options struct {
	// Addr is the listen address, for example "127.0.0.1:8080".
	Addr string
	// Passphrase, when set, must be sent in the X-RabbiMQ-Key header.
	Passphrase string
	// MaxBodyBytes caps the size of a published message body.
	// 0 means broker.DefaultMaxMessageBytes.
	MaxBodyBytes int64
}

// LocalServer is an HTTP server that provides an API for a local RabbiMQ instance.
type LocalServer struct {
	mq         api.RabbiMQ
	httpAddr   string
	passphrase string
	maxBody    int64
	handler    http.Handler

	mu     sync.Mutex
	server *http.Server
}

// NewLocalServer creates a new HTTP server for a local RabbiMQ instance.
func NewLocalServer(mq api.RabbiMQ, httpAddr string) *LocalServer {
	return NewLocalServerWithOptions(mq, Options{Addr: httpAddr})
}

// NewLocalServerWithAuth creates a new HTTP server for a local RabbiMQ instance with auth.
func NewLocalServerWithAuth(mq api.RabbiMQ, httpAddr, passphrase string) *LocalServer {
	return NewLocalServerWithOptions(mq, Options{Addr: httpAddr, Passphrase: passphrase})
}

// NewLocalServerWithOptions creates a new HTTP server for a local RabbiMQ instance.
func NewLocalServerWithOptions(mq api.RabbiMQ, opts Options) *LocalServer {
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = broker.DefaultMaxMessageBytes
	}
	s := &LocalServer{
		mq:         mq,
		httpAddr:   opts.Addr,
		passphrase: opts.Passphrase,
		maxBody:    opts.MaxBodyBytes,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/channels", s.handleChannels)
	mux.HandleFunc("/consume", s.handleConsume)
	mux.HandleFunc("/healthz", s.handleHealth)
	s.handler = mux
	return s
}

// Handler returns the HTTP handler serving the RabbiMQ API.
func (s *LocalServer) Handler() http.Handler {
	return s.handler
}

func (s *LocalServer) newServer() *http.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.server = &http.Server{
		Addr:              s.httpAddr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s.server
}

// ListenAndServe serves HTTP until Shutdown is called. It returns
// http.ErrServerClosed after a graceful shutdown.
func (s *LocalServer) ListenAndServe() error {
	log.Printf("HTTP server listening on %s", s.httpAddr)
	return s.newServer().ListenAndServe()
}

// ListenAndServeTLS serves HTTPS until Shutdown is called. It returns
// http.ErrServerClosed after a graceful shutdown.
func (s *LocalServer) ListenAndServeTLS(certFile, keyFile string) error {
	log.Printf("HTTPS server listening on %s", s.httpAddr)
	return s.newServer().ListenAndServeTLS(certFile, keyFile)
}

// Start starts the HTTP server in the background.
func (s *LocalServer) Start() {
	go func() {
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server failed: %v", err)
		}
	}()
}

// StartTLS starts the HTTPS server in the background using the provided certificate and key.
func (s *LocalServer) StartTLS(certFile, keyFile string) {
	go func() {
		if err := s.ListenAndServeTLS(certFile, keyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTPS server failed: %v", err)
		}
	}()
}

// Shutdown stops accepting connections and waits for in-flight requests.
// Long-lived consume streams end when ctx expires or the broker shuts down.
func (s *LocalServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func (s *LocalServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, "ok\n")
}

func (s *LocalServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) || !allowMethod(w, r, http.MethodPost) {
		return
	}

	task, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.writeError(w, broker.ErrMessageTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	channel := r.URL.Query().Get("channel")
	if err := s.mq.Publish(channel, task); err != nil {
		s.writeError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *LocalServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) || !allowMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, s.mq.Metrics())
}

func (s *LocalServer) handleChannels(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) || !allowMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, s.mq.Channels())
}

// handleConsume streams tasks as Server-Sent Events. With ?once=true it
// delivers a single task and closes the stream, so no further task is taken
// off the queue on behalf of a client that only wanted one.
func (s *LocalServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) || !allowMethod(w, r, http.MethodGet) {
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	once, err := parseBool(r.URL.Query().Get("once"))
	if err != nil {
		http.Error(w, "once must be a boolean", http.StatusBadRequest)
		return
	}

	tasks, err := s.mq.Consume(r.URL.Query().Get("channel"))
	if err != nil {
		s.writeError(w, err)
		return
	}

	if err := s.mq.AcquireConsumer(); err != nil {
		s.writeError(w, err)
		return
	}
	defer s.mq.ReleaseConsumer()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	// Send headers right away so clients know the subscription is live.
	if _, err := io.WriteString(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case task, ok := <-tasks:
			if !ok {
				// The broker is shutting down.
				return
			}
			if _, err := io.WriteString(w, formatEvent(task)); err != nil {
				return
			}
			flusher.Flush()
			if once {
				return
			}
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// formatEvent encodes a task as an SSE event. Each line of the payload becomes
// its own data field so payloads containing newlines keep their framing.
// SSE treats CR, LF and CRLF alike, so line endings arrive as LF.
func formatEvent(task []byte) string {
	payload := strings.ReplaceAll(string(task), "\r\n", "\n")
	payload = strings.ReplaceAll(payload, "\r", "\n")

	var b strings.Builder
	for _, line := range strings.Split(payload, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

func parseBool(v string) (bool, error) {
	if v == "" {
		return false, nil
	}
	return strconv.ParseBool(v)
}

// errorStatus maps broker errors to an HTTP status and error code.
func errorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, broker.ErrShabbatObserved):
		return http.StatusServiceUnavailable, CodeShabbat
	case errors.Is(err, broker.ErrQueueFull):
		return http.StatusTooManyRequests, CodeQueueFull
	case errors.Is(err, broker.ErrChannelFull):
		return http.StatusTooManyRequests, CodeChannelFull
	case errors.Is(err, broker.ErrChannelLimit):
		return http.StatusTooManyRequests, CodeChannelLimit
	case errors.Is(err, broker.ErrConsumerLimit):
		return http.StatusTooManyRequests, CodeConsumerLimit
	case errors.Is(err, broker.ErrMessageTooLarge):
		return http.StatusRequestEntityTooLarge, CodeMessageTooLarge
	case errors.Is(err, broker.ErrInvalidChannel):
		return http.StatusBadRequest, CodeInvalidChannel
	case errors.Is(err, broker.ErrClosed):
		return http.StatusServiceUnavailable, CodeUnavailable
	default:
		return http.StatusInternalServerError, CodeInternal
	}
}

func (s *LocalServer) writeError(w http.ResponseWriter, err error) {
	status, code := errorStatus(err)
	w.Header().Set(ErrorHeader, code)
	if code == CodeShabbat {
		if isShabbat, remaining := s.mq.ShabbatStatus(); isShabbat {
			seconds := int(math.Ceil(remaining.Seconds()))
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
		}
	}
	http.Error(w, err.Error(), status)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(append(data, '\n'))
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func (s *LocalServer) authorize(w http.ResponseWriter, r *http.Request) bool {
	if s.passphrase == "" {
		return true
	}
	got := r.Header.Get(AuthHeader)
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.passphrase)) != 1 {
		w.Header().Set(ErrorHeader, CodeUnauthorized)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
