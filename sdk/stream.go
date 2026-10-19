package sdk

import (
	"bufio"
	"io"
	"strings"
)

// maxEventLine bounds a single SSE line; messages are limited by the broker anyway.
const maxEventLine = 16 * 1024 * 1024

// eventReader parses a Server-Sent Events stream and yields the data of each event.
type eventReader struct {
	scanner *bufio.Scanner
}

func newEventReader(r io.Reader) *eventReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxEventLine)
	return &eventReader{scanner: scanner}
}

// next returns the data of the next event. Multi-line data is joined with
// "\n" as the SSE specification requires. It returns io.EOF when the stream
// ends cleanly.
func (e *eventReader) next() ([]byte, error) {
	var data []string
	hasData := false
	for e.scanner.Scan() {
		line := e.scanner.Text()
		if line == "" {
			if hasData {
				return []byte(strings.Join(data, "\n")), nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			// Comment, used for keep-alives.
			continue
		}

		field, value := line, ""
		if i := strings.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], line[i+1:]
			// A single leading space after the colon is not part of the value.
			value = strings.TrimPrefix(value, " ")
		}
		if field == "data" {
			data = append(data, value)
			hasData = true
		}
	}
	if err := e.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}
