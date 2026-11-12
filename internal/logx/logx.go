// Package logx implements the logging subsystem: levels, per-key rate
// limiting and size based rotation.
//
// The original lsyncd called fopen/ctime/fclose for every single log line and
// had no rate limiting at all, which turned a single stuck rsync into a 438 GB
// log file (issue #93). Here every line goes through one buffered writer and
// repetitive messages are collapsed.
package logx

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Level is a log severity.
type Level int

// Severity levels, ordered.
const (
	Debug Level = iota
	Info
	Warn
	Error
	None
)

// ParseLevel converts a config string into a Level.
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info", "normal":
		return Info, nil
	case "debug", "all":
		return Debug, nil
	case "warn", "warning", "scarce":
		return Warn, nil
	case "error":
		return Error, nil
	case "none", "off", "silent":
		return None, nil
	}
	return Info, fmt.Errorf("unknown log level %q", s)
}

func (l Level) String() string {
	switch l {
	case Debug:
		return "Debug"
	case Info:
		return "Normal"
	case Warn:
		return "Warning"
	case Error:
		return "Error"
	}
	return "None"
}

// Logger is a concurrency safe leveled logger with rate limiting.
type Logger struct {
	mu     sync.Mutex
	out    io.Writer
	closer io.Closer
	level  Level
	// throttle keeps one entry per rate limited message key.
	throttle map[string]*throttleState
	now      func() time.Time
}

type throttleState struct {
	last       time.Time
	suppressed int
}

// New builds a logger writing to w.
func New(w io.Writer, level Level) *Logger {
	return &Logger{out: w, level: level, throttle: make(map[string]*throttleState), now: time.Now}
}

// NewFile builds a logger writing to path, rotating at maxBytes (0 disables
// rotation) and keeping at most maxFiles archives.
func NewFile(path string, level Level, maxBytes int64, maxFiles int) (*Logger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	r, err := newRotator(path, maxBytes, maxFiles)
	if err != nil {
		return nil, err
	}
	l := New(r, level)
	l.closer = r
	return l, nil
}

// NewStderr builds a logger on stderr.
func NewStderr(level Level) *Logger { return New(os.Stderr, level) }

// Level returns the current level.
func (l *Logger) Level() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Enabled reports whether lv would be printed.
func (l *Logger) Enabled(lv Level) bool { return lv >= l.Level() }

// Logf writes one line.
func (l *Logger) Logf(lv Level, format string, args ...any) {
	if !l.Enabled(lv) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level == None {
		return
	}
	fmt.Fprintf(l.out, "%s %s: %s\n", l.now().Format("Mon Jan  2 15:04:05 2006"), lv, msg)
}

// Debugf logs at debug level.
func (l *Logger) Debugf(format string, args ...any) { l.Logf(Debug, format, args...) }

// Infof logs at info level.
func (l *Logger) Infof(format string, args ...any) { l.Logf(Info, format, args...) }

// Warnf logs at warn level.
func (l *Logger) Warnf(format string, args ...any) { l.Logf(Warn, format, args...) }

// Errorf logs at error level.
func (l *Logger) Errorf(format string, args ...any) { l.Logf(Error, format, args...) }

// Throttlef logs at most once per interval for a given key, appending the
// number of suppressed repeats. This is what keeps a stuck backend from
// filling the disk.
func (l *Logger) Throttlef(key string, interval time.Duration, lv Level, format string, args ...any) {
	if !l.Enabled(lv) {
		return
	}
	now := l.now()
	l.mu.Lock()
	st, ok := l.throttle[key]
	if !ok {
		st = &throttleState{}
		l.throttle[key] = st
		// Keys are built from sync names and error kinds, so this map is
		// normally tiny. Pruning keys that have been quiet for an hour keeps
		// it that way even if a caller ever keys on something unbounded.
		if len(l.throttle) > 1024 {
			for k, v := range l.throttle {
				// last.IsZero is the key just inserted below; without this
				// guard now.Sub(zero) > time.Hour is true and the prune
				// deletes the new key, so nothing is ever throttled once the
				// map exceeds the cap.
				if !v.last.IsZero() && v.suppressed == 0 && now.Sub(v.last) > time.Hour && k != key {
					delete(l.throttle, k)
				}
			}
		}
	}
	if !st.last.IsZero() && now.Sub(st.last) < interval {
		st.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := st.suppressed
	st.suppressed = 0
	st.last = now
	l.mu.Unlock()

	msg := fmt.Sprintf(format, args...)
	if suppressed > 0 {
		msg = fmt.Sprintf("%s (suppressed %d similar messages)", msg, suppressed)
	}
	l.Logf(lv, "%s", msg)
}

// ResetThrottle forgets rate limiting state, e.g. after a successful run so a
// later failure is reported immediately.
func (l *Logger) ResetThrottle(key string) {
	l.mu.Lock()
	delete(l.throttle, key)
	l.mu.Unlock()
}

// Flush pushes buffered log lines to the file without closing it.
//
// It is what a reload needs before syscall.Exec: the image is about to be
// replaced, but other goroutines may still log until the very end, and
// closing first would make those writes race a nil buffer. Exec keeps the
// file descriptor, so flushing is enough.
func (l *Logger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f, ok := l.out.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

// rotator is an io.Writer that rotates files by size.
//
// Lines go through a bufio.Writer: the original opened, wrote and closed the
// file for every single line, and a daemon that reports progress per file turns
// that into two syscalls per event. The buffer is flushed on rotation, on Close
// and at least once a second, so "tail -f" still shows what a running daemon is
// doing instead of waiting for 64 KB of output.
type rotator struct {
	mu       sync.Mutex
	path     string
	f        *os.File
	bw       *bufio.Writer
	size     int64
	maxBytes int64
	maxFiles int
	stop     chan struct{}
	stopOnce sync.Once
	// closed is set by Close. A nil bw with closed=false means a rotation
	// failed to reopen; the next Write retries instead of silently dropping
	// every later line.
	closed bool
}

// flushInterval bounds how long a line can sit in the buffer.
const flushInterval = time.Second

func newRotator(path string, maxBytes int64, maxFiles int) (*rotator, error) {
	r := &rotator{path: path, maxBytes: maxBytes, maxFiles: maxFiles, stop: make(chan struct{})}
	if err := r.open(); err != nil {
		return nil, err
	}
	go r.flusher()
	return r, nil
}

func (r *rotator) flusher() {
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.mu.Lock()
			if r.bw != nil {
				_ = r.bw.Flush()
			}
			r.mu.Unlock()
		}
	}
}

func (r *rotator) open() error {
	// O_NOFOLLOW: the log path is configured by the operator but lives in a
	// directory that may be writable by others; following a planted symlink
	// would append privileged log lines to an arbitrary file.
	f, err := openLogFile(r.path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	r.f = f
	r.bw = bufio.NewWriterSize(f, 64<<10)
	r.size = st.Size()
	return nil
}

func (r *rotator) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bw == nil {
		if r.closed {
			// A logger still referenced by a draining goroutine must drop the
			// line, not dereference a nil buffer and panic.
			return len(p), nil
		}
		// The previous rotation could not reopen the file (ENOSPC, read-only
		// remount). Retry here instead of discarding the rest of the log for
		// the lifetime of the process.
		if err := r.open(); err != nil {
			return 0, err
		}
	}
	// Buffered bytes are not in the file yet, so they have to be counted too:
	// otherwise a burst of lines smaller than the buffer never rotates.
	if r.maxBytes > 0 && r.size+int64(len(p)) > r.maxBytes {
		if err := r.bw.Flush(); err != nil {
			return 0, err
		}
		if err := r.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := r.bw.Write(p)
	r.size += int64(n)
	return n, err
}

func (r *rotator) rotate() error {
	if r.bw != nil {
		if err := r.bw.Flush(); err != nil {
			return err
		}
	}
	if r.maxFiles < 1 {
		// No archives requested: start the file over instead of unlinking it.
		// Removing it would work too, but the inode would change under any
		// "tail -f" that is watching the log, which is the moment an operator
		// is most likely to be watching it.
		if err := os.Truncate(r.path, 0); err != nil {
			return err
		}
		r.size = 0
		return nil
	}
	// Shift archives: path.N -> path.N+1
	for i := r.maxFiles - 1; i >= 1; i-- {
		old := fmt.Sprintf("%s.%d", r.path, i)
		next := fmt.Sprintf("%s.%d", r.path, i+1)
		if _, err := os.Stat(old); err == nil {
			_ = os.Rename(old, next)
		}
	}
	oldF := r.f
	if runtime.GOOS == "windows" {
		// Windows refuses to rename a file that is still open. Close it, and
		// if the reopen below fails Write retries from a nil buffer rather
		// than dropping the line.
		if oldF != nil {
			_ = oldF.Close()
		}
		r.f, r.bw = nil, nil
	}
	_ = os.Rename(r.path, r.path+".1")
	// Keep the old descriptor until the new file is open. The previous version
	// set bw=nil first; when open failed (ENOSPC, read-only remount) every
	// later Write was discarded as if it had succeeded, disabling logging for
	// the rest of the process lifetime.
	if err := r.open(); err != nil {
		if runtime.GOOS != "windows" {
			// Put the current file back under its name and keep appending.
			_ = os.Rename(r.path+".1", r.path)
		}
		return err
	}
	if oldF != nil && runtime.GOOS != "windows" {
		_ = oldF.Close()
	}
	return nil
}

// Flush pushes buffered bytes without closing the file.
func (r *rotator) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bw != nil {
		return r.bw.Flush()
	}
	return nil
}

func (r *rotator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopOnce.Do(func() { close(r.stop) })
	r.closed = true
	var err error
	if r.bw != nil {
		err = r.bw.Flush()
		r.bw = nil
	}
	if r.f != nil {
		if cerr := r.f.Close(); err == nil {
			err = cerr
		}
		r.f = nil
	}
	return err
}
