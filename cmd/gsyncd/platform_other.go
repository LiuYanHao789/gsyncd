//go:build !linux

package main

import "fmt"

// requireLinuxFeature refuses an option that would otherwise be a silent
// no-op on this platform.
func requireLinuxFeature(name string) error {
	return fmt.Errorf("%s is only supported on Linux", name)
}
