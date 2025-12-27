package sdk

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (c *Client) streamFrom(ctx context.Context, channel string) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/consume", nil)
		if err != nil {
			errCh <- err
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		if c.key != "" {
			req.Header.Set("X-RabbiMQ-Key", c.key)
		}
		if channel != "" {
			q := req.URL.Query()
			q.Set("channel", channel)
			req.URL.RawQuery = q.Encode()
		}

		resp, err := c.stream.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			errCh <- fmt.Errorf("stream failed: %s", strings.TrimSpace(string(body)))
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var dataLines []string
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if len(dataLines) > 0 {
					payload := strings.Join(dataLines, "\n")
					select {
					case <-ctx.Done():
						return
					case out <- []byte(payload):
					}
					dataLines = dataLines[:0]
				}
				continue
			}

			if strings.HasPrefix(line, "data:") {
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}

		if err := scanner.Err(); err != nil {
			errCh <- err
			return
		}
	}()

	return out, errCh
}
