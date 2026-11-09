package sync

import (
	"os"
	"testing"
	"time"
)

func TestTruncateMtimeNanos(t *testing.T) {
	const ns = int64(1700000000)*1e9 + 123456789
	cases := []struct {
		gran time.Duration
		want int64
	}{
		{0, ns},
		{time.Second, int64(1700000000) * 1e9},
		{time.Millisecond, int64(1700000000)*1e9 + 123000000},
		{time.Microsecond, int64(1700000000)*1e9 + 123456000},
		{time.Nanosecond, ns},
	}
	for _, c := range cases {
		if got := truncateMtimeNanos(ns, c.gran); got != c.want {
			t.Fatalf("truncate(%v) = %d, want %d", c.gran, got, c.want)
		}
	}
}

func TestProbeMtimeGranularityRestoresDirMtime(t *testing.T) {
	dir := t.TempDir()
	base := time.Unix(1700000000, 123000000)
	if err := os.Chtimes(dir, base, base); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = probeMtimeGranularity(dir)
	after, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("probe changed target directory mtime: before=%v after=%v", before.ModTime(), after.ModTime())
	}
}
