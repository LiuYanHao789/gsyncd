//go:build !linux

package watcher

import "time"

// NewAuto returns the best available backend for this platform. Outside Linux
// there is no portable recursive notification API that does not require extra
// dependencies, so the snapshot-diff backend is used. Its cost is proportional
// to the number of entries, which is why the interval defaults to something
// generous and is configurable.
func NewAuto(pollInterval time.Duration, logf func(string, ...any)) (Backend, error) {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}
	return NewPoll(pollInterval, logf), nil
}
