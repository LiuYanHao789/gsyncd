//go:build !unix && !windows

package main

func acquirePidLock(string) error { return nil }

func releasePidLock() {}
