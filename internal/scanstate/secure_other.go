//go:build !unix

package scanstate

import "os"

func openScanSegment(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}
