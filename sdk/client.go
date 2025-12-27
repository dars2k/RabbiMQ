package sdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// Client provides a typed API for RabbiMQ.
type Client struct {
	baseURL string
	client  *http.Client
	stream  *http.Client
	key     string
}

// NewClient creates a client from config values.
func NewClient(cfg Config) (*Client, error) {
	timeout, err := cfg.timeoutOrDefault()
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{}
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
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		stream: &http.Client{
			Transport: transport,
		},
		key: cfg.Passphrase,
	}, nil
}

// NewClientFromFile creates a client from a JSON config file.
func NewClientFromFile(path string) (*Client, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return NewClient(cfg)
}

// Publish sends a task to a specific channel.
func (c *Client) Publish(ctx context.Context, channel string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/publish", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.key != "" {
		req.Header.Set("X-RabbiMQ-Key", c.key)
	}
	if channel != "" {
		q := req.URL.Query()
		q.Set("channel", channel)
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed: %s", strings.TrimSpace(string(body)))
	}

	return nil
}

// Consume reads a single task from a specific channel using the consume endpoint.
func (c *Client) Consume(ctx context.Context, channel string) ([]byte, bool, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgs, errs := c.streamFrom(streamCtx, channel)
	select {
	case msg, ok := <-msgs:
		if !ok {
			return nil, false, nil
		}
		return msg, true, nil
	case err, ok := <-errs:
		if ok && err != nil {
			return nil, false, err
		}
		return nil, false, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
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
