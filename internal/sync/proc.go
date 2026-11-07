package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os/exec"
	"strings"
	"time"
)

// ErrTimeout is returned when a child exceeded its deadline.
var ErrTimeout = errors.New("process timed out")

// ExitError carries the exit status of a child that ran to completion and
// reported failure. Callers that can tell benign statuses from fatal ones need
// the code, not just an error.
type ExitError struct {
	Code int
	Tail string
}

func (e *ExitError) Error() string {
	if e.Tail == "" {
		return fmt.Sprintf("exit %d", e.Code)
	}
	return fmt.Sprintf("exit %d: %s", e.Code, e.Tail)
}

// ExitCode returns the exit status carried by err, or -1 when err is not an
// exit status at all (a timeout, a missing binary, a killed child).
func ExitCode(err error) int {
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return -1
}

// RunConfig describes one child process invocation.
type RunConfig struct {
	Path  string
	Args  []string
	Stdin io.Reader
	// Timeout kills the child when it runs too long. Zero means no limit.
	Timeout time.Duration
	// OnLine receives stderr lines, if set.
	OnLine func(line string)
	// MaxStderr caps how much stderr is retained for error messages.
	MaxStderr int
}

// ErrNotFound marks a missing binary, which must not be retried forever.
var ErrNotFound = errors.New("binary not found")

// maxLineLen bounds the partial-line buffer of the streaming reader.
//
// A child that emits one enormous "line" - a progress meter that uses carriage
// returns and never a newline, or a truncated write from a dying process - would
// otherwise make that buffer grow with the output. At 100 TB the retry loop is
// the only thing keeping a broken transfer from spinning, so this buffer must
// have a ceiling or a single misbehaving rsync becomes an OOM kill.
const maxLineLen = 64 << 10

// Run executes the process once.
//
// The child is placed in its own process group so a timeout takes down the
// whole tree. Without that, killing rsync leaves the ssh underneath it alive
// and, with rsyncssh, the remote rsync as well: over hours of a flapping
// network those accumulate until the box runs out of processes.
func Run(ctx context.Context, rc RunConfig) error {
	if _, err := exec.LookPath(rc.Path); err != nil {
		return fmt.Errorf("%w: %s", ErrNotFound, rc.Path)
	}
	cctx := ctx
	var cancel context.CancelFunc
	if rc.Timeout > 0 {
		cctx, cancel = context.WithTimeout(ctx, rc.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(cctx, rc.Path, rc.Args...)
	cmd.Stdin = rc.Stdin
	setProcGroup(cmd)
	cmd.Cancel = func() error {
		killGroup(cmd)
		return nil
	}
	if rc.MaxStderr == 0 {
		rc.MaxStderr = 8192
	}
	var tail strings.Builder
	if rc.OnLine != nil {
		pr, pw := io.Pipe()
		cmd.Stderr = pw
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 0, 4096)
			tmp := make([]byte, 4096)
			emit := func(line string) {
				if tail.Len() < rc.MaxStderr {
					tail.WriteString(line)
					tail.WriteByte('\n')
				}
				rc.OnLine(line)
			}
			for {
				n, err := pr.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
					buf = splitLines(buf, maxLineLen, emit)
				}
				if err != nil {
					return
				}
			}
		}()
		err := cmd.Run()
		pw.Close()
		<-done
		if err != nil {
			return wrapRunErr(cctx, err, tail.String())
		}
		return nil
	}
	// Not streaming: keep only the tail. CombinedOutput() would hold the whole
	// output, and "rsync -v" over a large batch prints one line per file.
	out := &cappedBuffer{max: rc.MaxStderr}
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()
	if err != nil {
		return wrapRunErr(cctx, err, string(out.buf))
	}
	return nil
}

// splitLines hands every complete line in buf to emit and returns the leftover
// partial line.
//
// A line that grows past budget without a newline is split rather than held: a
// progress meter that ends each update with a carriage return, or a process that
// dies mid-write, otherwise makes this buffer grow with the output.
func splitLines(buf []byte, budget int, emit func(string)) []byte {
	for {
		// bytes.IndexByte, not strings.IndexByte(string(buf)): converting on
		// every line copied the whole buffer and made a long rsync line
		// quadratic.
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			emit(string(buf[:i]))
			buf = append(buf[:0], buf[i+1:]...)
			continue
		}
		if len(buf) > budget {
			emit(string(buf[:budget]))
			buf = append(buf[:0], buf[budget:]...)
			continue
		}
		return buf
	}
}

// cappedBuffer keeps the last max bytes written to it and discards the rest.
type cappedBuffer struct {
	max int
	buf []byte
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if c.max <= 0 {
		return n, nil
	}
	if len(p) >= c.max {
		c.buf = append(c.buf[:0], p[len(p)-c.max:]...)
		return n, nil
	}
	if over := len(c.buf) + len(p) - c.max; over > 0 {
		c.buf = append(c.buf[:0], c.buf[over:]...)
	}
	c.buf = append(c.buf, p...)
	return n, nil
}

func wrapRunErr(ctx context.Context, err error, output string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s", ErrTimeout, lastLine(output))
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return &ExitError{Code: ee.ExitCode(), Tail: lastLine(output)}
	}
	return err
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(no output)"
	}
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	if len(s) > 400 {
		s = s[:400] + "..."
	}
	return s
}

// Backoff computes the retry delay: 1s, 2s, 4s ... capped, with jitter.
//
// This is the fix for issue #250, where a dead network made lsyncd respawn
// rsync as fast as the OS could fork, pinning a core at 100% and filling the
// log at 6 MB/s.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 5 * time.Minute
	}
	d := base << uint(attempt)
	if d <= 0 || d > max {
		d = max
	}
	jitter := time.Duration(rand.Int63n(int64(d/5 + 1)))
	return d + jitter
}
