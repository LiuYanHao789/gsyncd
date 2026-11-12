//go:build windows

package main

// Windows keeps the existing pidAlive check. A real lock would need
// LockFileEx; the engine is not deployed on Windows in production, and a
// wrong implementation is worse than the documented limitation.
func acquirePidLock(path string) error { return nil }

func releasePidLock() {}
