//go:build !linux

package sync

import "errors"

// Extended attributes (including POSIX ACLs) are only implemented for Linux.
// main rejects preserve_xattrs at startup on other platforms; returning an
// error keeps a direct call from silently claiming success.
func copyXattrs(string, string) error {
	return errors.New("extended attributes are only supported on Linux")
}
