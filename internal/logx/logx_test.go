package logx

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Warn)
	l.Debugf("dbg-line")
	l.Infof("inf-line")
	l.Warnf("warn-line")
	l.Errorf("err-line")
	out := buf.String()
	if strings.Contains(out, "dbg-line") || strings.Contains(out, "inf-line") {
		t.Fatalf("low levels leaked: %q", out)
	}
	if !strings.Contains(out, "warn-line") || !strings.Contains(out, "err-line") {
		t.Fatalf("high levels missing: %q", out)
	}
}

func TestParseLevelAliases(t *testing.T) {
	cases := map[string]Level{
		"": Info, "info": Info, "normal": Info,
		"debug": Debug, "all": Debug,
		"warn": Warn, "scarce": Warn,
		"error": Error, "none": None, "off": None, "silent": None,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil {
			t.Fatalf("ParseLevel(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
	// Levels are case insensitive, so only genuinely unknown names fail.
	if got, err := ParseLevel("WARN"); err != nil || got != Warn {
		t.Fatalf("ParseLevel(\"WARN\") = %v, %v", got, err)
	}
	for _, bad := range []string{"chatty", "verbose", "loud"} {
		if _, err := ParseLevel(bad); err == nil {
			t.Fatalf("ParseLevel(%q) should fail", bad)
		}
	}
}

// Throttlef must collapse repeated messages with the same key inside the
// window. This is what keeps a stuck backend from filling the disk: issue #93
// reported a 438GB log made of "waiting for N more child processes" lines.
func TestThrottleCollapsesWithinWindow(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Info)
	for i := 0; i < 100; i++ {
		l.Throttlef("waiting", time.Hour, Info, "waiting for %d children", 1)
	}
	if got := strings.Count(buf.String(), "waiting for 1 children"); got != 1 {
		t.Fatalf("expected exactly one emission, got %d: %q", got, buf.String())
	}
}

// Once the window elapses the message must come back, carrying the number of
// events it swallowed.
func TestThrottleReportsSuppressedCount(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Info)
	clock := time.Unix(1700000000, 0)
	l.now = func() time.Time { return clock }

	l.Throttlef("k", time.Minute, Info, "boom")
	for i := 0; i < 5; i++ {
		l.Throttlef("k", time.Minute, Info, "boom")
	}
	clock = clock.Add(2 * time.Minute)
	l.Throttlef("k", time.Minute, Info, "boom")

	out := buf.String()
	if n := strings.Count(out, "boom"); n != 2 {
		t.Fatalf("expected 2 emissions after window, got %d: %q", n, out)
	}
	if !strings.Contains(out, "suppressed 5 similar messages") {
		t.Fatalf("missing suppression summary: %q", out)
	}
}

func TestThrottleDifferentKeysIndependent(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Info)
	l.Throttlef("a", time.Hour, Info, "alpha")
	l.Throttlef("b", time.Hour, Info, "beta")
	if !strings.Contains(buf.String(), "alpha") || !strings.Contains(buf.String(), "beta") {
		t.Fatalf("keys must not suppress each other: %q", buf.String())
	}
}

func TestResetThrottle(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Info)
	l.Throttlef("k", time.Hour, Info, "once")
	l.ResetThrottle("k")
	l.Throttlef("k", time.Hour, Info, "once")
	if got := strings.Count(buf.String(), "once"); got != 2 {
		t.Fatalf("reset should allow a new emission, got %d", got)
	}
}

func TestConcurrentLoggingIsSafe(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Debug)
	var wg sync.WaitGroup
	const writers, perWriter = 8, 50
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				l.Infof("line")
			}
		}()
	}
	wg.Wait()
	if n := strings.Count(buf.String(), "line"); n != writers*perWriter {
		t.Fatalf("lost log lines: got %d, want %d", n, writers*perWriter)
	}
}

func TestRotationKeepsBoundedFiles(t *testing.T) {
	dir := t.TempDir()
	path := dir + string(os.PathSeparator) + "app.log"
	l, err := NewFile(path, Info, 512, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		l.Infof("padding padding padding padding %d", i)
	}
	l.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected rotation to create history files, got %v", entries)
	}
	if len(entries) > 4 {
		t.Fatalf("rotation kept too many files (%d): %v", len(entries), entries)
	}
}

// Buffering must not lose the tail and rotation must never cut a line in half.
// Rotation deliberately discards old archives (maxBytes/maxFiles is a bound, not
// a queue), so this asserts the two things that would be real bugs: every line
// found anywhere is complete, and the newest line survived Close.
func TestBufferedLinesSurviveCloseAndRotation(t *testing.T) {
	dir := t.TempDir()
	path := dir + string(os.PathSeparator) + "app.log"
	l, err := NewFile(path, Info, 256, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		l.Infof("line-%03d-%s", i, strings.Repeat("x", 32))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, e := range entries {
		b, err := os.ReadFile(dir + string(os.PathSeparator) + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
			if line == "" {
				continue
			}
			if !strings.Contains(line, "line-") || !strings.HasSuffix(line, strings.Repeat("x", 32)) {
				t.Fatalf("truncated line in %s: %q", e.Name(), line)
			}
			seen++
		}
	}
	if seen == 0 {
		t.Fatal("no complete line survived, buffering or rotation dropped everything")
	}
	last, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(last), "line-039-") {
		t.Fatalf("last line before Close was lost; current file ends with %q", tail(string(last), 120))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func TestNoLevelWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, None)
	l.Errorf("should not appear")
	if buf.Len() != 0 {
		t.Fatalf("level none must be silent, got %q", buf.String())
	}
}

func BenchmarkLogf(b *testing.B) {
	l := New(&bytes.Buffer{}, Info)
	l.now = func() time.Time { return time.Unix(1700000000, 0) }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Infof("applied %d changes", i)
	}
}

func BenchmarkThrottlef(b *testing.B) {
	l := New(&bytes.Buffer{}, Info)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Throttlef("waiting", time.Minute, Info, "waiting for %d children", 1)
	}
}
