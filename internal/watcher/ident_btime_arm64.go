//go:build linux && arm64

package watcher

const statxSyscall = 291

func statxSupported() bool { return true }
