//go:build !linux

package sync

import (
	"errors"
	"os"
)

// Recreating FIFOs and device nodes is implemented only on Linux. Returning an
// error (rather than nil) keeps a misconfigured copy_special from silently
// looking successful; main rejects the option at startup on this platform.
func makeSpecial(string, os.FileInfo, copyOptions) error {
	return errors.New("special files are only supported on Linux")
}
