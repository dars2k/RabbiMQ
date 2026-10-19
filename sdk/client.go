package sdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	authHeader  = "X-RabbiMQ-Key"
	errorHeader = "X-RabbiMQ-Error"
)

// Error codes reported by the server in APIError.Code.
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

// APIError is returned when the server rejects a request.
type APIError struct {
	// StatusCode is the HTTP status code.
	StatusCode int
	// Code is the machine-readable error code, for example CodeShabbat.
	// It is empty if the server did not provide one.
	Code string
	// Message is the human-readable error text from the server.
	Message string
	// RetryAfter is how long the server asked the client to wait, if it did.
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.StatusCode)
	}
	return fmt.Sprintf("rabbimq: %s (HTTP %d)", msg, e.StatusCode)
}

// Temporary reports whether retrying the same request later may succeed.
func (e *APIError) Temporary() bool {
	switch e.Code {
	case CodeShabbat, CodeQueueFull, CodeChannelFull, CodeChannelLimit, CodeConsumerLimit, CodeUnavailable:
		return true
	}
	return e.StatusCode == http.StatusServiceUnavailable || e.StatusCode == http.StatusTooManyRequests
}

// IsShabbat reports whether err was caused by the broker observing Shabbat.
func IsShabbat(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Code == CodeShabbat
}

// ErrStreamClosed is reported by Subscribe when the server ends the stream,
// for example because it is shutting down. Callers may reconnect.
var ErrStreamClosed = errors.New("rabbimq: stream closed by server")

// Metrics is a point-in-time view of broker metrics.
type Metrics struct {
	Published         uint64 `json:"published"`
	Delivered         uint64 `json:"delivered"`
	Rejected          uint64 `json:"rejected"`
	QueueDepth        int    `json:"queue_depth"`
	HeldDuringShabbat int    `json:"held_during_shabbat"`
	IsShabbat         bool   `json:"is_shabbat"`
	Consumers         int    `json:"consumers"`
	Channels          int    `json:"channels"`
}

// ChannelStats reports pending tasks for a channel.
type ChannelStats struct {
	Channel    string `json:"channel"`
	QueueDepth int    `json:"queue_depth"`
}

// Client provides a typed API for RabbiMQ.
type Client struct {
	baseURL string
	channel string
	client  *http.Client
	stream  *http.Client
	key     string
}

// NewClient creates a client from config values.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	timeout, err := cfg.timeoutOrDefault()
	if err != nil {
		return nil, err
	}

	// Start from the default transport so proxy settings and sane
	// connection timeouts are honored.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.InsecureSkipVerify || cfg.CACertFile != "" {
		tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
		if cfg.CACertFile != "" {
			pool, err := loadCertPool(cfg.CACertFile)
			if err != nil {
				return nil, err
			}
			tlsConfig.RootCAs = pool
		}
		transport.TLSClientConfig = tlsConfig
	}

	return &Client{
		baseURL: strings.TrimRight(cfg.Address, "/"),
		channel: cfg.Channel,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		// Streams are long-lived, so they are bounded by the caller's context only.
		stream: &http.Client{
			Transport: transport,
		},
		key: cfg.Passphrase,
	}, nil
}

// NewClientFromFile creates a client from a JSON or YAML config file.
func NewClientFromFile(path string) (*Client, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return NewClient(cfg)
}

// newRequest builds a request for path, using the configured default channel
// when channel is empty.
func (c *Client) newRequest(ctx context.Context, method, path, channel string, body io.Reader, query url.Values) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if c.key != "" {
		req.Header.Set(authHeader, c.key)
	}
	if query == nil {
		query = url.Values{}
	}
	if channel == "" {
		channel = c.channel
	}
	if channel != "" {
		query.Set("channel", channel)
	}
	req.URL.RawQuery = query.Encode()
	return req, nil
}

// Publish sends a task to a specific channel. An empty channel uses the
// configured default channel, or the broker's default channel if none is set.
func (c *Client) Publish(ctx context.Context, channel string, payload []byte) error {
	req, err := c.newRequest(ctx, http.MethodPost, "/publish", channel, bytes.NewReader(payload), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return newAPIError(resp)
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

// Consume waits for a single task from a channel and returns it.
// It returns ok=false without an error if the server closed the stream
// before a task arrived. Use Subscribe to receive tasks continuously.
func (c *Client) Consume(ctx context.Context, channel string) ([]byte, bool, error) {
	resp, err := c.openStream(ctx, channel, true)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	events := newEventReader(resp.Body)
	msg, err := events.next()
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return msg, true, nil
}

// Subscribe streams tasks from a channel until ctx is canceled or the stream
// fails. The message channel is closed when the subscription ends; if it
// ended for any reason other than ctx being canceled, the error is sent on
// the error channel first (ErrStreamClosed when the server ended the stream).
func (c *Client) Subscribe(ctx context.Context, channel string) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		resp, err := c.openStream(ctx, channel, false)
		if err != nil {
			if ctx.Err() == nil {
				errCh <- err
			}
			return
		}
		defer resp.Body.Close()

		events := newEventReader(resp.Body)
		for {
			msg, err := events.next()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, io.EOF) {
					err = ErrStreamClosed
				}
				errCh <- err
				return
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errCh
}

func (c *Client) openStream(ctx context.Context, channel string, once bool) (*http.Response, error) {
	query := url.Values{}
	if once {
		query.Set("once", "true")
	}
	req, err := c.newRequest(ctx, http.MethodGet, "/consume", channel, nil, query)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.stream.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, newAPIError(resp)
	}
	return resp, nil
}

// Metrics returns the broker's metrics.
func (c *Client) Metrics(ctx context.Context) (Metrics, error) {
	var m Metrics
	err := c.getJSON(ctx, "/metrics", &m)
	return m, err
}

// Channels returns the broker's channels and their pending task counts.
func (c *Client) Channels(ctx context.Context) ([]ChannelStats, error) {
	var stats []ChannelStats
	err := c.getJSON(ctx, "/channels", &stats)
	return stats, err
}

// Healthy reports whether the broker answers its health check.
func (c *Client) Healthy(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return newAPIError(resp)
	}
	return nil
}

func (c *Client) getJSON(ctx context.Context, path string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	if c.key != "" {
		req.Header.Set(authHeader, c.key)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return newAPIError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("rabbimq: decode %s: %w", path, err)
	}
	return nil
}

func newAPIError(resp *http.Response) *APIError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Code:       resp.Header.Get(errorHeader),
		Message:    strings.TrimSpace(string(body)),
	}
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		apiErr.RetryAfter = time.Duration(seconds) * time.Second
	}
	return apiErr
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("failed to parse CA cert: %s", path)
	}
	return pool, nil
}
