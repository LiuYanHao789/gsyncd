//go:build !unix && !windows

package main

// No fork/setsid equivalent on plan9 or js/wasm; always run in the foreground.
const daemonSupported = false

func daemonize(string) (bool, error) { return false, nil }
