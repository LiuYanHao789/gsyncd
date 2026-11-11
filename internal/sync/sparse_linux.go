//go:build linux

package sync

import (
	"errors"
	"hash"
	"io"
	"os"
	"syscall"
)

// Linux SEEK_DATA/SEEK_HOLE values. They are stable ABI but are not exported
// by the syscall package on every Go version, so they are named here.
const (
	seekData = 3
	seekHole = 4
)

var errSparseUnsupported = errors.New("sparse copy not supported by this filesystem")

func sparseSupported() bool { return true }

// sparseSupportedOn probes one file before a copy starts. SEEK_DATA is
// supported by the common Linux filesystems (ext4, XFS, Btrfs, tmpfs) and
// returns ENXIO on a file with no data; EINVAL means the filesystem does not
// implement it and the caller falls back to an ordinary copy.
func sparseSupportedOn(f *os.File) bool {
	_, err := f.Seek(0, seekData)
	_, _ = f.Seek(0, io.SeekStart)
	return err == nil || errors.Is(err, syscall.ENXIO)
}

// copySparse copies n bytes, writing only data extents and seeking over holes,
// so the destination stays sparse. The hash receives the full logical content
// (holes contribute zero bytes), which keeps transfer-time verification
// meaningful even when the destination is sparse.
func copySparse(dst *os.File, src *os.File, n int64, h hash.Hash, lim *rateLimiter) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	var off int64
	// The zero buffer is only needed to hash holes; do not allocate 64 KiB per
	// file when verification is disabled.
	var zero []byte
	for off < n {
		data, err := src.Seek(off, seekData)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				// No data after off: the remainder is a hole.
				data = n
			} else if errors.Is(err, syscall.EINVAL) {
				return off, errSparseUnsupported
			} else {
				return off, err
			}
		}
		if data > n {
			data = n
		}
		if data > off {
			// Hole [off, data): hash zeros and extend the destination with a
			// seek instead of writing blocks.
			if h != nil && zero == nil {
				zero = make([]byte, 64<<10)
			}
			hashZeros(h, zero, data-off)
			if _, err := dst.Seek(data-off, io.SeekCurrent); err != nil {
				return off, err
			}
			off = data
		}
		if off >= n {
			break
		}
		hole, err := src.Seek(off, seekHole)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				hole = n
			} else if errors.Is(err, syscall.EINVAL) {
				return off, errSparseUnsupported
			} else {
				return off, err
			}
		}
		if hole > n {
			hole = n
		}
		if hole <= off {
			// A filesystem that reports no hole after data: copy the rest.
			hole = n
		}
		length := hole - off
		var r io.Reader = io.NewSectionReader(src, off, length)
		if lim != nil {
			r = newThrottledReader(r, lim)
		}
		w, err := copyBounded(dst, r, length, h)
		if err != nil {
			return off, err
		}
		if w < length {
			// The source shrank while it was being read. Stop where the
			// bytes ended instead of extending the destination with zeros the
			// source never had.
			return off + w, nil
		}
		off = hole
	}
	// A trailing hole is represented by a seek, which does not extend the
	// file on its own; Truncate makes the size right without allocating
	// blocks.
	if err := dst.Truncate(n); err != nil {
		return off, err
	}
	return off, nil
}

// hashZeros feeds n zero bytes to h without allocating a buffer of that size.
func hashZeros(h hash.Hash, zero []byte, n int64) {
	if h == nil {
		return
	}
	for n > 0 {
		chunk := int64(len(zero))
		if n < chunk {
			chunk = n
		}
		_, _ = h.Write(zero[:chunk])
		n -= chunk
	}
}
