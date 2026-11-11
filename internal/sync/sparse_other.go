//go:build !linux

package sync

import (
	"errors"
	"hash"
	"os"
)

var errSparseUnsupported = errors.New("sparse copy not supported by this platform")

func sparseSupported() bool { return false }

func sparseSupportedOn(*os.File) bool { return false }

// copySparse is never reached when sparseSupported is false; the signature
// exists so copyFileOnce stays platform independent.
func copySparse(*os.File, *os.File, int64, hash.Hash, *rateLimiter) (int64, error) {
	return 0, errSparseUnsupported
}
