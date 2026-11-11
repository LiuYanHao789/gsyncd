//go:build linux && amd64

package watcher

const statxSyscall = 332

func statxSupported() bool { return true }
