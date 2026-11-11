//go:build linux && !amd64 && !arm64

package watcher

const statxSyscall = 0

func statxSupported() bool { return false }
