package sdk

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestEventReader(t *testing.T) {
	stream := strings.Join([]string{
		": comment",
		"",
		"data: one",
		"",
		"event: custom",
		"id: 7",
		"data:no-space",
		"",
		"data: multi",
		"data:  two spaces",
		"",
		"data",
		"",
		"data: trailing without blank line",
	}, "\n")

	r := newEventReader(strings.NewReader(stream))
	want := []string{"one", "no-space", "multi\n two spaces", ""}
	for _, w := range want {
		got, err := r.next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if string(got) != w {
			t.Fatalf("got %q, want %q", got, w)
		}
	}
	// An event that is not terminated by a blank line is incomplete.
	if _, err := r.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestEventReaderLineTooLong(t *testing.T) {
	long := "data: " + strings.Repeat("x", maxEventLine+1) + "\n\n"
	if _, err := newEventReader(strings.NewReader(long)).next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("expected scanner error, got %v", err)
	}
}
