// Package verify compares a source tree with a mirror target.
//
// Tree is the structural check: every directory is read on both sides and the
// sorted entry names/types are merged, so a missing directory or file is
// reported by path. It never stats an individual file, which is what makes it
// affordable on an NFS target; readdir is the only target operation.
//
// Content is the optional deep check: every selected source file is hashed and
// the target file is hashed and compared. It reads the entire mirror and is
// meant for manual or low-frequency runs, not for every sync.
package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Result is the outcome of one comparison.
type Result struct {
	Checked       int
	Missing       int
	Extra         int
	TypeMismatch  int
	Mismatch      int
	MtimeMismatch int
	Samples       []string
}

// OK reports whether the two sides matched.
func (r Result) OK() bool {
	return r.Missing == 0 && r.Extra == 0 && r.TypeMismatch == 0 && r.Mismatch == 0 && r.MtimeMismatch == 0
}

// Options configures Tree.
type Options struct {
	// Ignore skips a path relative to the source root (slash separated). It is
	// the same predicate the sync itself uses.
	Ignore func(rel string, dir bool) bool
	// SampleLimit caps how many problem paths are retained for logging.
	SampleLimit int
}

const targetMarker = ".gsyncd-target"
const tmpPrefix = ".gsyncd-tmp-"

func (r *Result) sample(s string, limit int) {
	if limit <= 0 {
		limit = 20
	}
	if len(r.Samples) < limit {
		r.Samples = append(r.Samples, s)
	}
}

// Tree compares the directory structure and entry sets of source and target.
func Tree(source, target string, opts Options) (Result, error) {
	var r Result
	if opts.SampleLimit <= 0 {
		opts.SampleLimit = 20
	}
	if err := r.compare(source, target, ".", opts); err != nil {
		return r, err
	}
	return r, nil
}

func (r *Result) compare(srcDst, dstDst, rel string, opts Options) error {
	srcEnts, err := os.ReadDir(srcDst)
	if err != nil {
		return err
	}
	dstEnts, err := os.ReadDir(dstDst)
	if err != nil {
		if os.IsNotExist(err) {
			r.missingTree(srcDst, rel, opts)
			return nil
		}
		return err
	}
	srcEnts = filterEntries(srcEnts, rel, opts, false)
	dstEnts = filterEntries(dstEnts, rel, opts, true)
	i, j := 0, 0
	for i < len(srcEnts) || j < len(dstEnts) {
		switch {
		case j >= len(dstEnts):
			r.missing(srcDst, srcEnts[i], rel, opts)
			i++
		case i >= len(srcEnts):
			r.extraPath(dstEnts[j], rel, opts)
			j++
		default:
			sc, dc := srcEnts[i], dstEnts[j]
			switch {
			case sc.Name() < dc.Name():
				r.missing(srcDst, sc, rel, opts)
				i++
			case sc.Name() > dc.Name():
				r.extraPath(dc, rel, opts)
				j++
			default:
				r.Checked++
				sk, dk := kind(sc), kind(dc)
				if sk != dk {
					r.TypeMismatch++
					r.sample("type: "+joinRel(rel, sc.Name())+" source="+sk+" target="+dk, opts.SampleLimit)
				} else if sc.IsDir() {
					if err := r.compare(filepath.Join(srcDst, sc.Name()), filepath.Join(dstDst, dc.Name()), joinRel(rel, sc.Name()), opts); err != nil {
						return err
					}
				}
				i++
				j++
			}
		}
	}
	return nil
}

func (r *Result) missing(srcDst string, e os.DirEntry, rel string, opts Options) {
	p := joinRel(rel, e.Name())
	r.Missing++
	r.sample("missing: "+p, opts.SampleLimit)
	if e.IsDir() {
		r.missingTree(filepath.Join(srcDst, e.Name()), p, opts)
	}
}

// missingTree counts everything below a source directory that has no target
// counterpart. It is called on a relative path to avoid carrying absolute
// source paths through the counters.
func (r *Result) missingTree(srcDst, rel string, opts Options) {
	ents, err := os.ReadDir(srcDst)
	if err != nil {
		return
	}
	for _, e := range ents {
		crel := joinRel(rel, e.Name())
		if opts.Ignore != nil && opts.Ignore(crel, e.IsDir()) {
			continue
		}
		r.Missing++
		r.sample("missing: "+crel, opts.SampleLimit)
		if e.IsDir() {
			r.missingTree(filepath.Join(srcDst, e.Name()), crel, opts)
		}
	}
}

func (r *Result) extraPath(e os.DirEntry, rel string, opts Options) {
	p := joinRel(rel, e.Name())
	r.Extra++
	r.sample("extra: "+p, opts.SampleLimit)
}

func filterEntries(ents []os.DirEntry, rel string, opts Options, target bool) []os.DirEntry {
	out := ents[:0]
	for _, e := range ents {
		name := e.Name()
		if target {
			if rel == "." && name == targetMarker {
				continue
			}
			if strings.HasPrefix(name, tmpPrefix) {
				continue
			}
		}
		crel := joinRel(rel, name)
		if opts.Ignore != nil && opts.Ignore(crel, e.IsDir()) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func joinRel(rel, name string) string {
	if rel == "." {
		return name
	}
	return rel + "/" + name
}

func kind(e os.DirEntry) string {
	switch {
	case e.IsDir():
		return "dir"
	case e.Type()&os.ModeSymlink != 0:
		return "symlink"
	case e.Type().IsRegular():
		return "file"
	default:
		return "special"
	}
}

// ContentOptions configures Content.
type ContentOptions struct {
	Ignore func(rel string, dir bool) bool
	// Workers bounds concurrent file comparisons.
	Workers int
	// SamplePercent, when in (0,100), selects that share of files
	// deterministically by path. Zero or 100 means every file.
	SamplePercent int
	SampleLimit   int
	// MtimeGranularity is the target filesystem's timestamp resolution. Zero
	// probes it (one tiny temporary file on the target); a filesystem that
	// stores only whole seconds then compares mtimes at second resolution.
	MtimeGranularity time.Duration
}

// Content hashes every selected regular file on both sides and reports
// missing files and hash mismatches. Extra target files are the structural
// check's job, not this one's.
func Content(source, target string, opts ContentOptions) (Result, error) {
	var r Result
	if opts.SampleLimit <= 0 {
		opts.SampleLimit = 20
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	gran := opts.MtimeGranularity
	if gran == 0 {
		gran = probeGranularity(target)
	}
	type job struct{ rel, src string }
	jobs := make(chan job, opts.Workers*4)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for w := 0; w < opts.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 256<<10)
			for j := range jobs {
				srcInfo, err := os.Lstat(j.src)
				if err != nil {
					if os.IsNotExist(err) {
						mu.Lock()
						r.Missing++
						r.sample("missing: "+j.rel, opts.SampleLimit)
						mu.Unlock()
						continue
					}
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				if !srcInfo.Mode().IsRegular() {
					continue
				}
				dst := filepath.Join(target, filepath.FromSlash(j.rel))
				dstInfo, err := os.Lstat(dst)
				if err != nil {
					if os.IsNotExist(err) {
						mu.Lock()
						r.Missing++
						r.sample("missing: "+j.rel, opts.SampleLimit)
						mu.Unlock()
						continue
					}
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				if !dstInfo.Mode().IsRegular() {
					mu.Lock()
					r.TypeMismatch++
					r.sample("type: "+j.rel+" target="+kindOf(dstInfo), opts.SampleLimit)
					mu.Unlock()
					continue
				}
				sh, err := hashFile(j.src, buf)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				dh, err := hashFile(dst, buf)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				r.Checked++
				if sh != dh {
					r.Mismatch++
					r.sample("hash: "+j.rel+" source="+sh+" target="+dh, opts.SampleLimit)
				} else if truncateNanos(srcInfo.ModTime().UnixNano(), gran) != truncateNanos(dstInfo.ModTime().UnixNano(), gran) {
					r.MtimeMismatch++
					r.sample("mtime: "+j.rel, opts.SampleLimit)
				}
				mu.Unlock()
			}
		}()
	}

	walkErr := filepath.WalkDir(source, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == source {
			return nil
		}
		rel, rerr := filepath.Rel(source, p)
		if rerr != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if opts.Ignore != nil && opts.Ignore(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		// The worker lstats each subject; d_type can be DT_UNKNOWN on some
		// filesystems, and opening a FIFO to hash it would block forever.
		if opts.SamplePercent > 0 && opts.SamplePercent < 100 {
			if int(crc32.ChecksumIEEE([]byte(rel)))%100 >= opts.SamplePercent {
				return nil
			}
		}
		jobs <- job{rel: rel, src: p}
		return nil
	})
	close(jobs)
	wg.Wait()
	if walkErr != nil {
		return r, walkErr
	}
	if firstErr != nil {
		return r, firstErr
	}
	return r, nil
}

func kindOf(info os.FileInfo) string {
	switch {
	case info.IsDir():
		return "dir"
	case info.Mode()&os.ModeSymlink != 0:
		return "symlink"
	case info.Mode().IsRegular():
		return "file"
	default:
		return "special"
	}
}

// probeGranularity measures the target's timestamp resolution by setting a
// known nanosecond value on one temporary file. A target that cannot be
// written falls back to exact comparison.
func probeGranularity(dir string) time.Duration {
	// The probe file changes the directory mtime; restore it so a later
	// daemon start still sees the checkpoint evidence it had before.
	pre, _ := os.Stat(dir)
	f, err := os.CreateTemp(dir, ".gsyncd-mtime-*")
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

func truncateNanos(ns int64, gran time.Duration) int64 {
	if gran <= 0 {
		return ns
	}
	g := int64(gran)
	return ns / g * g
}

func hashFile(path string, buf []byte) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
