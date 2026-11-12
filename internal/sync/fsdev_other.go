//go:build !unix

package sync

import "os"

// differentFilesystem is not available on this platform; require the marker.
func differentFilesystem(a, b os.FileInfo) bool { return false }
