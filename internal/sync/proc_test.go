package sync

import (
	"strings"
	"testing"
)

// Complete lines are handed out as they arrive and the partial tail is kept.
func TestSplitLinesEmitsCompleteLines(t *testing.T) {
	var got []string
	rest := splitLines([]byte("a\nbb\nccc"), 64, func(s string) { got = append(got, s) })
	if len(got) != 2 || got[0] != "a" || got[1] != "bb" {
		t.Fatalf("want [a bb], got %q", got)
	}
	if string(rest) != "ccc" {
		t.Fatalf("leftover %q", rest)
	}
}

// A line that never ends must be cut at the budget instead of growing the
// buffer with the child: an endless progress meter is an OOM otherwise.
func TestSplitLinesCapsUnterminatedLine(t *testing.T) {
	var got []string
	bytes := 0
	rest := splitLines([]byte(strings.Repeat("x", 200)), 64, func(s string) {
		got = append(got, s)
		bytes += len(s)
	})
	if len(got) != 3 || bytes != 192 || len(rest) != 8 {
		t.Fatalf("want three 64 byte pieces and 8 left over, got %d pieces, %d bytes, rest %d",
			len(got), bytes, len(rest))
	}
	for _, s := range got {
		if len(s) != 64 {
			t.Fatalf("piece is %d bytes", len(s))
		}
	}
}

// Empty lines are still lines.
func TestSplitLinesKeepsEmptyLines(t *testing.T) {
	var got []string
	rest := splitLines([]byte("\n\nx\n"), 64, func(s string) { got = append(got, s) })
	if len(got) != 3 || got[0] != "" || got[1] != "" || got[2] != "x" {
		t.Fatalf("got %q", got)
	}
	if len(rest) != 0 {
		t.Fatalf("leftover %q", rest)
	}
}

// The retained stderr tail is bounded no matter how much the child prints.
func TestCappedBufferKeepsTail(t *testing.T) {
	c := &cappedBuffer{max: 8}
	if n, err := c.Write([]byte("0123456789")); n != 10 || err != nil {
		t.Fatalf("Write reported %d %v", n, err)
	}
	if string(c.buf) != "23456789" {
		t.Fatalf("after one oversized write: %q", c.buf)
	}
	c.Write([]byte("ab"))
	if string(c.buf) != "456789ab" {
		t.Fatalf("after a growing write: %q", c.buf)
	}
	zero := &cappedBuffer{}
	if n, _ := zero.Write([]byte("anything")); n != 8 || len(zero.buf) != 0 {
		t.Fatalf("a disabled cap must discard, got %d %q", n, zero.buf)
	}
}
