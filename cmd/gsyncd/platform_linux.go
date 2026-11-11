//go:build linux

package main

// requireLinuxFeature is a no-op on Linux, where xattrs/ACLs and mknod are
// implemented.
func requireLinuxFeature(string) error { return nil }
