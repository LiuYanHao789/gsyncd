package sync

import (
	"io"
	"sync"
	"time"
)

// rateLimiter is a process-wide (per backend) cumulative byte-rate limiter.
//
// One limiter is shared by all copy workers of a sync. A per-file limiter
// would give every worker the full configured rate, so 16 workers at
// bandwidth_limit = "10m" would move ~160 MiB/s. The cumulative model keeps
// the average at the configured rate regardless of concurrency.
type rateLimiter struct {
	mu    sync.Mutex
	rate  int64
	start time.Time
	total int64
}

func newRateLimiter(bytesPerSecond int64) *rateLimiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	return &rateLimiter{rate: bytesPerSecond, start: time.Now()}
}

// wait blocks until n bytes are allowed by the cumulative budget.
func (l *rateLimiter) wait(n int) {
	if l == nil || n <= 0 {
		return
	}
	l.mu.Lock()
	l.total += int64(n)
	want := time.Duration(float64(l.total) / float64(l.rate) * float64(time.Second))
	elapsed := time.Since(l.start)
	l.mu.Unlock()
	if want > elapsed {
		time.Sleep(want - elapsed)
	}
}

type throttledReader struct {
	r   io.Reader
	lim *rateLimiter
}

func newThrottledReader(r io.Reader, lim *rateLimiter) io.Reader {
	if lim == nil {
		return r
	}
	return &throttledReader{r: r, lim: lim}
}

func (t *throttledReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.lim.wait(n)
	}
	return n, err
}
