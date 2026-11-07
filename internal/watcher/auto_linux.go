//go:build linux

package watcher

import "time"

// NewAuto returns the best available backend for this platform. On Linux that
// is inotify; the polling interval is ignored.
func NewAuto(pollInterval time.Duration, logf func(string, ...any)) (Backend, error) {
	return NewInotify(logf)
}
