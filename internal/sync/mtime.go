package sync

import (
	"os"
	"time"
)

// probeMtimeGranularity measures how precisely the target filesystem stores
// modification times. ext4 images made with 128-byte inodes, FAT, some NFS
// exports and a few FUSE filesystems truncate to the second; comparing a
// nanosecond source timestamp against them would either re-copy every file on
// every pass or make a checkpoint never match.
//
// It writes and removes one tiny file. A read-only target simply yields zero
// ("no known granularity"), which falls back to exact nanosecond comparison.
func probeMtimeGranularity(dir string) time.Duration {
	// Creating and removing the probe changes the directory mtime, which is
	// exactly the evidence the checkpoint uses. Remember the previous value
	// and put it back afterwards; only the probe's own change is undone.
	pre, _ := os.Stat(dir)
	f, err := createTemp(dir, ".gsyncd-mtime-*")
	if err != nil {
		return 0
	}
	name := f.Name()
	_ = f.Close()
	defer func() {
		_ = os.Remove(name)
		if pre != nil {
			_ = os.Chtimes(dir, pre.ModTime(), pre.ModTime())
		}
	}()

	want := time.Unix(1700000000, 123456789)
	if err := os.Chtimes(name, want, want); err != nil {
		return 0
	}
	st, err := os.Stat(name)
	if err != nil {
		return 0
	}
	ns := st.ModTime().Nanosecond()
	switch {
	case ns == 0:
		return time.Second
	case ns%1_000_000 == 0:
		return time.Millisecond
	case ns%1_000 == 0:
		return time.Microsecond
	default:
		return time.Nanosecond
	}
}

// truncateMtimeNanos rounds a Unix-nanosecond timestamp down to the target
// filesystem's resolution. A zero granularity means "compare exactly".
func truncateMtimeNanos(ns int64, gran time.Duration) int64 {
	if gran <= 0 {
		return ns
	}
	g := int64(gran)
	return ns / g * g
}

func truncateMtime(t time.Time, gran time.Duration) time.Time {
	if gran <= 0 {
		return t
	}
	return time.Unix(0, truncateMtimeNanos(t.UnixNano(), gran))
}
