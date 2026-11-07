package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
	"gsyncd/internal/scanstate"
)

// Direct copies files itself, without spawning any external program.
//
// This is the default backend and the reason gsyncd needs no rsync, no ssh and
// no libc beyond what the Go runtime already provides: one static binary is the
// whole installation. It is also far cheaper for the "one file changed" case,
// where the original lsyncd forked a full rsync process that then re-walked
// the directory tree.
type Direct struct {
	Source string
	Target string
	// Delete removes target entries that no longer exist on the source.
	Delete bool
	// DeleteOnStartup controls the prune that runs during Startup; lsyncd's
	// delete = "running" suppresses it so a pre-populated target survives the
	// first pass.
	DeleteOnStartup bool
	// DeleteOnRun controls whether a delete seen while running is propagated;
	// delete = "startup" disables it.
	DeleteOnRun bool
	// Workers is the number of concurrent copy goroutines.
	Workers int
	// Ignore decides whether a path (relative, slash separated) is excluded.
	// It is consulted by the initial walk so that direct mode honours the same
	// exclude list the event filter uses; without it the very first copy of a
	// 100 TB tree would drag in every cache and build directory.
	Ignore func(rel string, dir bool) bool
	// Fsync flushes every copied file to stable storage before the rename.
	// Off by default because on a large tree it multiplies the copy cost by
	// an order of magnitude; large files are flushed regardless.
	Fsync bool
	// Logf receives diagnostics.
	Logf func(level string, format string, args ...any)

	// scan is the optional checkpoint store. When it is set the initial pass
	// records, per source directory, a digest of its direct children and the
	// directory's own mtime; a later start can then skip the target side of a
	// directory that provably has not changed. See scanstate.
	scan *scanstate.Store

	// Prefetch is the number of workers that read directory listings ahead of
	// the walk. Zero means "pick one" (see prefetchWorkers); a negative value
	// turns the prefetching off and restores the plain serial filepath.Walk.
	//
	// It is separate from Workers because the two compete for the same disk: a
	// cold startup is already saturating it with copies, and there is no point
	// reading ahead faster than the copies drain.
	Prefetch int

	st Stats
	// pruned counts removals seen by pruneDir so the log can be sampled
	// without a per-entry message (issue #93: 438 GB of log).
	pruned int
	// freshTarget records that the target was empty when the last Startup
	// began. It turns off the per-file "already identical?" stat and the
	// per-directory "already exists?" lstat, which together are one wasted
	// syscall per source entry: half a million of them on a 500k directory
	// tree, and nothing can match in an empty directory.
	//
	// It is only an optimisation. If the guess is wrong - another instance
	// writing into the same target - the worst case is an unnecessary copy
	// of a file that happened to be identical, never a missed one.
	freshTarget bool
}

// NewDirect builds a direct backend.
func NewDirect(source, target string, del bool, workers int, logf func(string, string, ...any)) *Direct {
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers > 16 {
			workers = 16
		}
	}
	if logf == nil {
		logf = func(string, string, ...any) {}
	}
	return &Direct{
		Source: source, Target: target,
		Delete: del, DeleteOnStartup: del, DeleteOnRun: del,
		Workers: workers, Logf: logf,
	}
}

// Name implements Backend.
func (d *Direct) Name() string { return "direct" }

// Stats returns a snapshot.
//
// The counters are bumped with atomic adds from the copy workers, so they are
// read back atomically rather than through the mutex: a struct copy under the
// lock still races with the workers, which never take it.
func (d *Direct) Stats() Stats {
	return Stats{
		Batches: atomic.LoadUint64(&d.st.Batches),
		Files:   atomic.LoadUint64(&d.st.Files),
		Deletes: atomic.LoadUint64(&d.st.Deletes),
		Errors:  atomic.LoadUint64(&d.st.Errors),
		Retries: atomic.LoadUint64(&d.st.Retries),
	}
}

// Close implements Backend.
func (d *Direct) Close() error { return nil }

// UseCheckpoint attaches a checkpoint store to the backend.
//
// It is a setter rather than a NewDirect parameter because the checkpoint is
// an optimisation nobody has to opt into: with no store attached the walk runs
// exactly as it did before, and every existing caller keeps working.
func (d *Direct) UseCheckpoint(st *scanstate.Store) { d.scan = st }

// Startup performs the initial full copy.
//
// The target prune is folded into the same walk that does the copying (see
// walkCopy) instead of running as a separate whole-tree pass.
func (d *Direct) Startup(ctx context.Context) error {
	if err := os.MkdirAll(d.Target, 0o755); err != nil {
		return err
	}
	d.pruned = 0
	d.freshTarget = targetIsEmpty(d.Target)
	defer func() { d.freshTarget = false }()
	if d.scan == nil {
		return d.walkCopy(ctx)
	}
	// A new segment starts here, so a run that follows an overflow-triggered
	// full resync re-reads the old checkpoint instead of appending to the tail
	// of the previous pass.
	if err := d.scan.Begin(); err != nil {
		d.Logf("warn", "scan state: cannot start %s: %v", d.scan.Path(), err)
	}
	err := d.walkCopy(ctx)
	// The new segment only replaces the old one when the pass ran to the end.
	// A cancelled or failed pass leaves the previous checkpoint in place, and a
	// partial segment is still usable because a record is only written for a
	// directory whose content is already known.
	d.scan.Finish(err == nil && ctx.Err() == nil)
	return err
}

// pruneDir removes target entries in dstDir that have no counterpart in
// srcDir. It is the "delete = startup" behaviour, and it doubles as the repair
// for drift accumulated while gsyncd was not running.
//
// The obvious implementation - walk the whole target and lstat the matching
// source path for every entry - costs two syscalls per entry and a second full
// traversal of a 100 TB tree. Reconciling one directory at a time during the
// copy pass costs one extra readdir per directory and needs no second walk at
// all, so the startup delete is effectively free.
//
// The per-entry log line is also a trap the original fell into: issue #93 is a
// single machine that wrote 438 GB of "removing ..." messages while a large
// tree was reconciled. Only the first few and then every 10000th are logged.
func (d *Direct) pruneDir(ctx context.Context, srcDir, dstDir string, dstEnts []os.DirEntry) error {
	if dstEnts == nil {
		var err error
		dstEnts, err = os.ReadDir(dstDir)
		if err != nil {
			// Only a missing target is uninteresting: the directory has not
			// been created yet, so it has nothing to prune. Anything else
			// (EACCES, EIO) means we cannot tell what is extra, and silently
			// skipping would leave behind entries this run was supposed to
			// delete - permanently, since nothing else revisits them.
			if !errors.Is(err, os.ErrNotExist) {
				d.Logf("warn", "cannot read target directory %s while pruning, extras there are left in place: %v", dstDir, err)
			}
			return nil
		}
	}
	srcEnts, err := os.ReadDir(srcDir)
	if err != nil {
		// The source vanished between the walk and this call (a burst delete),
		// so there is no longer any notion of "extra" for this directory. Any
		// other error means the keep list is unknown: pruning on it would
		// delete everything, so the directory is skipped and reported.
		if !errors.Is(err, os.ErrNotExist) {
			d.Logf("warn", "cannot read source directory %s while pruning, skipping its target: %v", srcDir, err)
		}
		return nil
	}
	keep := make(map[string]struct{}, len(srcEnts))
	for _, e := range srcEnts {
		keep[e.Name()] = struct{}{}
	}
	for _, e := range dstEnts {
		if _, ok := keep[e.Name()]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		p := filepath.Join(dstDir, e.Name())
		d.pruned++
		if d.pruned <= 10 || d.pruned%10000 == 0 {
			d.Logf("info", "removing extra %s", p)
		}
		// One stubborn entry (a file held open on Windows, a permission
		// denied directory) must not stop the prune of everything else in
		// this directory: the rest of the tree still has to converge.
		if err := os.RemoveAll(p); err != nil {
			d.Logf("warn", "cannot remove extra %s: %v", p, err)
			continue
		}
		atomic.AddUint64(&d.st.Deletes, 1)
	}
	return nil
}

// tmpPrefix marks the temporary files a copy stages before renaming into place.
const tmpPrefix = ".gsyncd-tmp-"

// tmpMaxAge is how old an abandoned staging file has to be before it is
// removed. A copy of a huge file can legitimately take hours, so this is
// generous: the cost of leaving a stale file a bit longer is a few bytes, the
// cost of deleting a live one is a failed transfer.
const tmpMaxAge = 24 * time.Hour

// sweepTemp deletes staging files that a previous run abandoned.
//
// copyFile always removes its temp file on the error paths, but a killed
// process cannot: SIGKILL, a power cut, or the OOM killer leave
// ".gsyncd-tmp-*" lying in the target directory forever. Nothing ever renames
// them into place, and `delete = startup` would remove them only if the whole
// directory were pruned, so on a long lived mirror they accumulate steadily.
func (d *Direct) sweepTemp(ctx context.Context, dir string, ents []os.DirEntry) {
	// Only files old enough to be certainly abandoned. A second gsyncd pointed
	// at the same target, or this one after a restart, may have a copy in
	// flight; deleting a young staging file would fail that transfer for no
	// reason.
	cutoff := time.Now().Add(-tmpMaxAge)
	for _, e := range ents {
		name := e.Name()
		if len(name) <= len(tmpPrefix) || name[:len(tmpPrefix)] != tmpPrefix {
			continue
		}
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		full := filepath.Join(dir, name)
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			d.Logf("debug", "cannot remove stale staging file %s: %v", full, err)
			continue
		}
		d.Logf("info", "removed stale staging file %s", full)
	}
}

// maxQueuedRecords bounds the checkpoint's reorder buffer.
//
// A directory's entry set is only complete once the whole subtree below it has
// been walked, so records finish in post-order while the store can only append
// them in walk order. This buffer holds the difference. Past the bound the pass
// records fewer directories: a shorter checkpoint costs the next start some
// re-verification, which is the safe direction to fail in.
const maxQueuedRecords = 1 << 15

// maxHeldFiles bounds how many files one directory may hold back while its
// digest is still undecided. A directory with more entries than this is treated
// as changed and streams to the workers normally.
const maxHeldFiles = 1 << 14

// scanCommitEvery is how many records accumulate before the checkpoint is
// flushed and fsynced.
//
// Every record describes a directory whose files are already in place, so a
// crash between commits costs at most this many re-verifications and never a
// skipped file. Committing per record would fsync 500000 times on a 100 TB
// tree; this keeps the loss window small and the syscall count negligible.
const scanCommitEvery = 4096

// walkItem is one file handed to the copy workers.
type walkItem struct {
	rel  string
	info os.FileInfo
	pd   *pending
}

// pending is one directory whose final timestamp waits for its own files.
//
// The record itself is written as soon as the walk leaves the directory, since
// everything it holds (names, sizes, mtimes) is known by then. What is not known
// is when the last file lands, and the directory's own mtime has to be set after
// that or the very act of copying into it would undo it. So the timestamp work
// is deferred to whoever finishes last: the walking goroutine if nothing was
// queued, otherwise the last worker to finish a file of that directory.
//
// Nothing ever waits on a pending, which is why the walk can run at full speed
// even when a single directory holds one enormous file.
type pending struct {
	dst    string
	mtime  time.Time
	left   int32
	sealed int32
	failed int32
	once   sync.Once
}

// finish stamps the target directory with the source directory's mtime.
//
// This is what makes a checkpoint verifiable from the target side without
// reading it: the expected timestamp is the source's, and anything that touches
// the mirror afterwards (a stray write, a hand deletion, a restore from an older
// backup) changes it. It also matches what rsync -a does to directory times.
func (p *pending) finish() {
	p.once.Do(func() {
		if atomic.LoadInt32(&p.failed) == 1 {
			// An entry failed, so the directory does not hold what the record
			// will claim. Leaving the timestamp alone makes it fail its check
			// next time and get reconciled again, which is the safe direction.
			return
		}
		// A coarse or read-only target filesystem lands here. The cost is that
		// this directory never matches its record and is re-verified on every
		// start, never that it is skipped.
		_ = os.Chtimes(p.dst, p.mtime, p.mtime)
	})
}

func (p *pending) add(n int32) { atomic.AddInt32(&p.left, n) }

func (p *pending) fileDone(failed bool) {
	if failed {
		atomic.StoreInt32(&p.failed, 1)
	}
	if atomic.AddInt32(&p.left, -1) == 0 && atomic.LoadInt32(&p.sealed) == 1 {
		p.finish()
	}
}

// discard retires a directory that needs no timestamp work because the
// checkpoint matched and the target is already correct.
func (p *pending) discard() {
	p.once.Do(func() {})
	atomic.StoreInt32(&p.sealed, 1)
}

func (p *pending) seal() {
	atomic.StoreInt32(&p.sealed, 1)
	if atomic.LoadInt32(&p.left) == 0 {
		p.finish()
	}
}

// walkFrame is one source directory being folded during the initial pass.
type walkFrame struct {
	rel  string
	src  string
	dst  string
	perm os.FileMode
	hash *scanstate.Hasher
	cnt  uint32
	// mt is the source directory's own mtime in Unix nanoseconds, taken when
	// the walk entered it. It is what the record stores and what the target
	// copy is stamped with, so the two can be compared without reading either
	// side's entries.
	mt int64
	pd *pending
	// hold means the checkpoint matched on both the source and the target
	// mtime, so this directory's files are being kept here until the digest
	// decides. On a hit they are dropped and never copied at all.
	hold bool
	base scanstate.Record
	todo []walkItem
}

// pathWithin reports whether rel is dir itself or lives inside it. Both are
// relative to the sync root and slash separated; "." is the root.
func pathWithin(rel, dir string) bool {
	if dir == "." || rel == dir {
		return true
	}
	return len(rel) > len(dir) && rel[len(dir)] == '/' && rel[:len(dir)] == dir
}

// reconcileDir brings one target directory in line with its source: extra
// entries are removed and staging files left by an interrupted copy are swept.
// A missing target directory is created when mk is set.
func (d *Direct) reconcileDir(ctx context.Context, src, dst string, perm os.FileMode, mk bool, note func(error)) {
	ents, err := os.ReadDir(dst)
	if err != nil {
		if mk {
			note(mkdirOnce(dst, perm|0o700))
		}
		return
	}
	d.sweepTemp(ctx, dst, ents)
	if d.Delete && d.DeleteOnStartup {
		note(d.pruneDir(ctx, src, dst, ents))
	}
}

// walkCopy mirrors the whole tree.
//
// Directories are created on the walking goroutine, in walk order, so a parent
// always exists before its children. That lets the copy workers skip the
// per-file MkdirAll the naive version did: on a ten million file tree that call
// alone was one extra stat per file, i.e. ten million syscalls that produce no
// useful work.
//
// When a checkpoint is configured the same walk also decides, per directory,
// whether the target side needs to be touched at all. See scanstate.
func (d *Direct) walkCopy(ctx context.Context) error {
	// A missing source root is not the same as an empty one, and conflating
	// them is destructive in one direction and silent in the other.
	//
	// filepath.Walk on a path that does not exist calls the walk function once
	// with an error and we swallow it (a subtree may legitimately vanish
	// mid-scan), so the whole pass became a no-op that still reported "initial
	// sync done". Observed on a real machine: the source root was removed, a
	// queue overflow forced a full resync, and the target kept all 91500 files
	// while the log said the sync succeeded.
	//
	// The other direction is worse: treating a missing root as empty would make
	// pruneDir delete the entire target. An unmounted filesystem or a typo in
	// "source" would then wipe the mirror. Refusing is the only safe answer.
	if _, err := os.Stat(d.Source); err != nil {
		return fmt.Errorf("source root is not usable, refusing to sync: %w", err)
	}
	// Without a checkpoint the walk behaves exactly as it did before: no
	// frames, no digests, no per-directory bookkeeping.
	tracking := d.scan != nil

	ch := make(chan walkItem, 1024)
	// Failures are recorded rather than dropped: the initial copy either
	// completed or it did not, and reporting "initial sync done" over a tree
	// that is missing files is exactly the kind of silent divergence this tool
	// exists to prevent.
	var firstErr error
	var errMu sync.Mutex
	note := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	var wg sync.WaitGroup
	for i := 0; i < d.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range ch {
				err := d.copyFileAt(ctx, it.rel, it.info)
				if err != nil {
					d.Logf("debug", "copy %s: %v", it.rel, err)
					note(err)
				}
				// Reported on failure too: the failing item marks the directory
				// itself, and it is the only thing that can.
				if it.pd != nil {
					it.pd.fileDone(err != nil)
				}
			}
		}()
	}

	// frames is the chain of directories the walk is currently inside, outermost
	// first. Walk order guarantees the last one is the immediate parent of the
	// entry being visited.
	var frames []*walkFrame
	// queued holds finished records that cannot be appended yet because an
	// ancestor is still open: a child's record is not writable before its
	// parent's, and the parent's own content is not final until the whole
	// subtree below it has been walked.
	var queued []scanstate.Record
	var emitted, hits, checked, dropped int

	flush := func() {
		for len(queued) > 0 {
			if len(frames) > 0 && pathWithin(queued[0].Path, frames[len(frames)-1].rel) {
				return
			}
			d.scan.Add(queued[0])
			queued = queued[1:]
			emitted++
			if emitted%scanCommitEvery == 0 {
				if err := d.scan.Commit(); err != nil {
					d.Logf("warn", "scan state: cannot write %s: %v", d.scan.Path(), err)
				}
			}
		}
	}

	closeFrame := func(fr *walkFrame) error {
		if !tracking {
			// Frames exist for the walk's own bookkeeping only; without a
			// checkpoint there is no digest to close and nothing to record.
			return nil
		}
		fresh := scanstate.Record{
			Path:  fr.rel,
			Mtime: fr.mt,
			Count: fr.cnt,
			Hash:  fr.hash.Sum(),
		}
		hit := fr.hold && fresh.Mtime == fr.base.Mtime &&
			fresh.Count == fr.base.Count && fresh.Hash == fr.base.Hash
		rec := fresh
		if hit {
			// Nothing below this directory changed and the target still carried
			// the timestamp the record expects, so the files held above were
			// never copied and are dropped here. The target is not touched at
			// all: no mkdir, no readdir, no prune, no timestamp.
			hits++
			fr.pd.add(-int32(len(fr.todo)))
			fr.todo = nil
			fr.pd.discard()
		} else {
			if fr.hold {
				// The digest disagreed after all, so the target work skipped on
				// the strength of the record has to happen now, before this
				// directory's files land.
				d.reconcileDir(ctx, fr.src, fr.dst, fr.perm, true, note)
			}
			// Held files keep the completion count they were given when they
			// were held: they are released to the workers here rather than
			// dropped, and whoever copies each one reports it.
			if n := len(fr.todo); n > 0 {
				for i := range fr.todo {
					fr.todo[i].pd = fr.pd
					select {
					case ch <- fr.todo[i]:
					case <-ctx.Done():
						fr.todo = nil
						return ctx.Err()
					}
				}
				fr.todo = nil
			}
			fr.pd.seal()
		}
		// A parent sorts before every descendant in walk order, and this
		// directory's descendants are already the contiguous tail of the queue.
		i := len(queued)
		for i > 0 && pathWithin(queued[i-1].Path, fr.rel) {
			i--
		}
		queued = append(queued, scanstate.Record{})
		copy(queued[i+1:], queued[i:])
		queued[i] = rec
		if len(queued) > maxQueuedRecords {
			// Whatever is stuck at the front belongs to a subtree the walk is
			// still inside. Dropping it shortens the checkpoint, and a shorter
			// checkpoint only costs the next start some re-verification.
			cut := len(queued) - maxQueuedRecords
			dropped += cut
			queued = queued[cut:]
		}
		flush()
		return nil
	}

	// fold records one entry into its parent directory's digest. Names are
	// always folded; contents only for regular files. A subdirectory's own
	// mtime is deliberately left out, otherwise one write in a leaf would
	// invalidate every directory above it and a checkpoint on a tree that is
	// being written to would never match again.
	fold := func(name string, info os.FileInfo) {
		if !tracking || len(frames) == 0 {
			return
		}
		fr := frames[len(frames)-1]
		fr.cnt++
		fr.hash.Name(name)
		if info.IsDir() {
			fr.hash.Dir()
			return
		}
		if info.Mode().IsRegular() {
			fr.hash.File(info.Size(), info.ModTime().UnixNano())
		}
	}

	// The walk reads listings ahead of the visit, which on a cold cache is the
	// difference between one metadata request in flight and a full queue of
	// them: 126s versus 15.9s for 210k directories. Visit order is unchanged,
	// which the checkpoint depends on.
	walkErr := walkOrdered(ctx, d.Source, prefetchWorkers(d.Prefetch), 0, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			// A vanishing subtree during the initial scan must not abort
			// the whole transfer; the watcher will report the change.
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rel, rerr := filepath.Rel(d.Source, p)
		if rerr != nil {
			return nil
		}
		// Walk visits a directory before its children and never comes back to
		// it, so a frame is finished the moment a path outside it is visited.
		for len(frames) > 0 {
			top := frames[len(frames)-1]
			if top.rel == rel || pathWithin(rel, top.rel) {
				break
			}
			frames = frames[:len(frames)-1]
			if cerr := closeFrame(top); cerr != nil {
				return cerr
			}
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// Symlinks are not mirrored (see the backend documentation); the
			// name is still folded so that adding or removing one shows up as
			// a change instead of going unnoticed.
			fold(info.Name(), info)
			return nil
		}
		if d.Ignore != nil && d.Ignore(filepath.ToSlash(rel), info.IsDir()) {
			if info.IsDir() {
				// SkipDir also stops the walk from descending, which is the
				// point: an excluded directory is not scanned at all.
				return filepath.SkipDir
			}
			return nil
		}
		fold(info.Name(), info)
		dir := filepath.Join(d.Target, rel)
		if info.IsDir() {
			fr := &walkFrame{
				rel:  rel,
				src:  p,
				dst:  dir,
				perm: info.Mode().Perm(),
				mt:   info.ModTime().UnixNano(),
			}
			if tracking {
				fr.hash = scanstate.NewHasher()
				fr.pd = &pending{dst: dir, mtime: info.ModTime()}
				// The record is only usable when the target directory still
				// carries the timestamp the record expects: that is the only
				// evidence available without reading the target, and any hand
				// edit, stray write or restore changes it.
				if base, ok := d.scan.Lookup(rel); ok {
					checked++
					if st, serr := os.Stat(dir); serr == nil && st.IsDir() &&
						st.ModTime().UnixNano() == base.Mtime {
						fr.hold = true
						fr.base = base
					}
				}
			}
			switch {
			case fr.hold:
				// The record and the target agree, so the target is neither
				// read nor pruned and its timestamp is left alone.
			case d.freshTarget:
				// Empty target: there is nothing to compare against and nothing
				// to prune, so not even the readdir is worth issuing. On a cold
				// 50 million directory tree that skip is 50 million syscalls.
				note(mkdirOnce(dir, fr.perm|0o700))
			default:
				// One ReadDir answers both "does it exist" and "what must go";
				// a Lstat followed by a second ReadDir would be two syscalls for
				// the same answer.
				d.reconcileDir(ctx, p, dir, fr.perm, true, note)
			}
			frames = append(frames, fr)
			return nil
		}
		var pd *pending
		if tracking && len(frames) > 0 {
			parent := frames[len(frames)-1]
			pd = parent.pd
			if parent.hold && len(parent.todo) < maxHeldFiles {
				// Held, not copied: if the digest below confirms the record,
				// this file never has to be looked at again.
				parent.todo = append(parent.todo, walkItem{rel: rel, info: info})
				pd.add(1)
				return nil
			}
		}
		if pd != nil {
			pd.add(1)
		}
		select {
		case ch <- walkItem{rel: rel, info: info, pd: pd}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	// Frames left open at the end are the last chain of directories; closing
	// them may still release held files to the workers, so it has to happen
	// before the channel is closed.
	for len(frames) > 0 {
		fr := frames[len(frames)-1]
		frames = frames[:len(frames)-1]
		if cerr := closeFrame(fr); cerr != nil && walkErr == nil {
			walkErr = cerr
		}
	}
	close(ch)
	wg.Wait()
	if tracking {
		flush()
		if err := d.scan.Commit(); err != nil {
			d.Logf("warn", "scan state: cannot write %s: %v", d.scan.Path(), err)
		}
		d.Logf("info", "scan state: %d directories verified, %d reused, %d recorded, %d dropped",
			checked, hits, emitted, dropped)
	}
	if walkErr != nil {
		return walkErr
	}
	return firstErr
}

// targetIsEmpty reports whether dir holds no entries at all.
//
// It is a heuristic, not a lock: something may write into the target a moment
// later. It is only ever used to skip work that would have been a no-op.
func targetIsEmpty(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		return false
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	return err != nil || len(names) == 0
}

// mkdirOnce creates dir without first asking whether it exists.
//
// os.MkdirAll stats every component of the path before creating anything, which
// on the initial walk of a deep tree is a stat per directory for a directory it
// is about to create anyway. The walk visits parents before children, so a plain
// Mkdir is enough; the MkdirAll fallback covers a target whose parents were
// pruned underneath us.
func mkdirOnce(dir string, perm os.FileMode) error {
	err := os.Mkdir(dir, perm)
	if err == nil || errors.Is(err, os.ErrExist) {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		if mkErr := os.MkdirAll(dir, perm); mkErr == nil {
			return nil
		}
	}
	return err
}

// copyFileAt copies one regular file whose parent directory the walk already
// created.
func (d *Direct) copyFileAt(ctx context.Context, rel string, info os.FileInfo) error {
	dst := filepath.Join(d.Target, rel)
	// Skip when the destination is already identical (size and mtime). On a
	// fresh target nothing can match, so the stat is skipped entirely.
	if !d.freshTarget {
		if st, err := os.Stat(dst); err == nil && st.Size() == info.Size() && st.ModTime().Equal(info.ModTime()) {
			return nil
		}
	}
	err := copyFile(d.Source+string(filepath.Separator)+rel, dst, info, d.Fsync)
	if errors.Is(err, os.ErrNotExist) {
		// The directory is missing despite the walk order: another sync
		// sharing this target removed it, or the walk skipped it because
		// it was unreadable. Recreate and retry once.
		if mkErr := os.MkdirAll(filepath.Dir(dst), 0o755); mkErr == nil {
			err = copyFile(d.Source+string(filepath.Separator)+rel, dst, info, d.Fsync)
		}
	}
	if err != nil {
		atomic.AddUint64(&d.st.Errors, 1)
		return err
	}
	atomic.AddUint64(&d.st.Files, 1)
	return nil
}

// fsyncThreshold is the size above which a copied file is always flushed to
// stable storage before being renamed into place.
//
// Not flushing at all is tempting: on a million small files the fsync calls cost
// more wall time than the copying itself. But a rename that reaches the disk
// before the data does leaves a zero length file behind after a power loss, and
// the next run skips it because size and mtime already match. Above this
// threshold the data is worth the flush; below it a lost file is recopied
// cheaply on the next pass.
const fsyncThreshold = 8 << 20

// copyBufferSize is the size of the buffer used to move file contents.
//
// Why it is not 32 KiB: that is what io.Copy falls back to when neither side
// can take a fast path, and io.CopyN guarantees it, because wrapping the source
// in a LimitReader hides the *os.File that copy_file_range would have handled.
// On the test machine the same device moved 100 MiB in 6.5s with 32 KiB
// requests and 3.1s with 4 MiB requests - the per-request overhead, not the
// bandwidth, was the limit. A megabyte is where that curve flattens; going
// further only makes the buffers harder to pool.
//
// The buffers are pooled because a copy worker allocates one per file and a
// million-file tree would otherwise hand the garbage collector a million
// megabyte allocations.
const copyBufferSize = 1 << 20

var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufferSize)
		return &b
	},
}

// copyBounded copies at most n bytes from src to dst and reports how many it
// moved. Reaching EOF before n bytes is not an error: the file shrank, and the
// next event will bring the rest.
//
// It exists instead of io.CopyN because io.CopyN both wraps the reader (killing
// the sendfile/copy_file_range path) and, once that path is gone, silently uses
// a 32 KiB buffer. Both of those are invisible in the code and both cost real
// throughput on a spinning or virtualised device.
func copyBounded(dst io.Writer, src io.Reader, n int64) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	bp := copyBufPool.Get().(*[]byte)
	buf := *bp
	// A small file does not need a megabyte of buffer, and asking for one to
	// move 200 bytes would touch pages that are never used.
	if int64(len(buf)) > n {
		buf = buf[:n]
	}
	defer func() {
		// Restore the full length before returning it: the pool hands the same
		// slice to the next caller, which expects its capacity back.
		*bp = (*bp)[:cap(*bp)]
		copyBufPool.Put(bp)
	}()

	var written int64
	for written < n {
		want := n - written
		if int64(len(buf)) < want {
			want = int64(len(buf))
		}
		r, err := src.Read(buf[:want])
		if r > 0 {
			w, werr := dst.Write(buf[:r])
			written += int64(w)
			if werr != nil {
				return written, werr
			}
			if w != r {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF {
				return written, nil
			}
			return written, err
		}
	}
	return written, nil
}

// copyFile writes to a temporary file in the destination directory and renames
// it into place, so a reader on the target never sees a half written file.
// fsyncAll forces the flush for small files too.
func copyFile(src, dst string, info os.FileInfo, fsyncAll bool) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, tmpPrefix+"*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	// Copy exactly the length that was stat'ed, not "everything until EOF".
	// The caller decided to copy based on that size and will stamp that
	// modification time on the result; if the file is still being written to,
	// an unbounded copy would pull in bytes that the stamp then misdescribes,
	// and the next pass could conclude the file is already up to date. A short
	// read only happens if the file shrank, which is harmless: the next event
	// brings it back.
	if _, err := copyBounded(tmp, in, info.Size()); err != nil {
		tmp.Close()
		return err
	}
	if fsyncAll || info.Size() >= fsyncThreshold {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	_ = os.Chmod(tmpName, info.Mode().Perm())
	_ = os.Chtimes(tmpName, info.ModTime(), info.ModTime())
	if err := os.Rename(tmpName, dst); err != nil {
		// Windows refuses to rename over an existing file in some cases.
		if rmErr := os.Remove(dst); rmErr == nil {
			err = os.Rename(tmpName, dst)
		}
		if err != nil {
			return err
		}
	}
	tmpName = ""
	return nil
}

// Apply implements Backend.
func (d *Direct) Apply(ctx context.Context, b Batch) error {
	atomic.AddUint64(&d.st.Batches, 1)

	if b.Full {
		return d.Startup(ctx)
	}
	if len(b.Changes) == 0 {
		return nil
	}

	// Renames first: they are cheap and free the old name before a possible
	// copy of a file with the same path.
	var renames, rest []aggregate.Change
	for _, c := range b.Changes {
		if c.From != "" && c.Op&event.Delete == 0 {
			renames = append(renames, c)
		} else {
			rest = append(rest, c)
		}
	}
	// Every failure has to reach the caller, not just the first one that fits
	// in a buffered channel. A batch that reports success while some of its
	// files were never written leaves the mirror silently wrong until the next
	// full resync, which on a 100 TB tree can be hours away. So the workers
	// record into one guarded slot and the batch fails if anything was lost.
	var firstErr error
	var errMu sync.Mutex
	note := func(err error) {
		if err == nil {
			return
		}
		atomic.AddUint64(&d.st.Errors, 1)
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	for _, c := range renames {
		if err := d.applyRename(ctx, c); err != nil {
			// applyRename already fell back to a copy; a failure here means
			// both paths failed.
			d.Logf("warn", "rename %s -> %s: %v", c.From, c.Path, err)
			note(err)
		}
	}

	// A buffered handoff, not an unbuffered one: with a synchronous channel the
	// dispatcher stops between every change to hand it to a worker, so a single
	// goroutine caps the batch rate no matter how many workers there are.
	work := make(chan aggregate.Change, d.Workers*4)
	var wg sync.WaitGroup
	for i := 0; i < d.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				if err := d.applyOne(ctx, c); err != nil {
					d.Logf("debug", "apply %s: %v", c.Path, err)
					note(err)
				}
			}
		}()
	}
	var skipped int
	started := time.Now()
	for i, c := range rest {
		if i > 0 && i%50000 == 0 {
			// A batch of a million changes can run for hours. One line every
			// 50k makes progress visible without flooding the log (#93), and
			// the elapsed time shows whether it is moving at all.
			d.Logf("info", "applying %d/%d changes, %s elapsed", i, len(rest), time.Since(started).Round(time.Millisecond))
		}
		select {
		case work <- c:
		case <-ctx.Done():
			// Shutting down. The engine flushes and re-adds a failed batch, so
			// reporting the cancellation is what keeps the rest of these
			// changes from vanishing.
			skipped++
		}
	}
	close(work)
	wg.Wait()
	if skipped > 0 {
		d.Logf("warn", "%d changes not applied: %v", skipped, ctx.Err())
		return ctx.Err()
	}
	return firstErr
}

// relInside resolves a change path against the source and refuses anything
// outside it. Every path a backend touches is joined onto the target, so a path
// that escapes the source would make filepath.Join produce something outside the
// destination — writing or deleting data the operator never pointed at. Events
// are routed before they get here; this is the belt to that suspenders.
func (d *Direct) relInside(p string) (string, error) {
	rel, ok := relInSource(d.Source, p)
	if !ok {
		return "", fmt.Errorf("%w: %s is outside the source %s", errOutside, p, d.Source)
	}
	return rel, nil
}

// errOutside marks a change that does not belong to this sync. It is not a
// transfer failure: retrying it would fail the same way forever and the batch
// would never drain.
var errOutside = errors.New("path outside the sync root")

// skipOutside turns an out-of-root change into a logged no-op.
func skipOutside(logf func(string, string, ...any), err error) error {
	if errors.Is(err, errOutside) {
		logf("debug", "skipping %v", err)
		return nil
	}
	return err
}

func (d *Direct) applyRename(ctx context.Context, c aggregate.Change) error {
	relFrom, err := d.relInside(c.From)
	if err != nil {
		return skipOutside(d.Logf, err)
	}
	relTo, err := d.relInside(c.Path)
	if err != nil {
		return skipOutside(d.Logf, err)
	}
	srcDst := filepath.Join(d.Target, relFrom)
	dstDst := filepath.Join(d.Target, relTo)

	if err := os.MkdirAll(filepath.Dir(dstDst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(srcDst, dstDst); err == nil {
		atomic.AddUint64(&d.st.Files, 1)
		return nil
	}
	// Cross-device or missing source on the target: fall back to copy+remove.
	if info, serr := os.Lstat(c.Path); serr == nil {
		if info.IsDir() {
			// Ignore takes root-relative paths, copyTree hands out paths
			// relative to the subtree it is copying: rebase, or every rule
			// silently matches nothing and a moved directory smuggles its
			// excluded contents onto the target.
			var ign func(rel string, dir bool) bool
			if d.Ignore != nil {
				ign = func(rel string, dir bool) bool {
					return d.Ignore(filepath.ToSlash(filepath.Join(relTo, rel)), dir)
				}
			}
			if err := copyTree(c.Path, dstDst, d.Fsync, ign); err != nil {
				return err
			}
		} else if err := copyFile(c.Path, dstDst, info, d.Fsync); err != nil {
			return err
		}
	}
	_ = os.RemoveAll(srcDst)
	atomic.AddUint64(&d.st.Files, 1)
	return nil
}

func (d *Direct) applyOne(ctx context.Context, c aggregate.Change) error {
	rel, err := d.relInside(c.Path)
	if err != nil {
		return skipOutside(d.Logf, err)
	}
	dst := filepath.Join(d.Target, rel)

	if c.Op&event.Delete != 0 && c.Op&(event.Create|event.Write) == 0 {
		if !d.DeleteOnRun {
			return nil
		}
		if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		atomic.AddUint64(&d.st.Deletes, 1)
		return nil
	}

	info, err := os.Lstat(c.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Vanished between the event and now: treat as a delete.
			if !d.DeleteOnRun {
				return nil
			}
			if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			atomic.AddUint64(&d.st.Deletes, 1)
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if info.IsDir() {
		if err := os.MkdirAll(dst, info.Mode().Perm()|0o700); err != nil {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	// A write or create event is proof the content changed, so it is copied
	// unconditionally. Do not "verify" it with size and mtime: filesystems cap
	// timestamp resolution (1 ms on XFS here, 1 s on ext3, 2 s on FAT), and an
	// application that writes twice inside one tick leaves the source looking
	// identical to the stale copy on the target. The event is the authority; the
	// stat is only a hint for the case where nothing but attributes moved.
	if c.Op&(event.Write|event.Create) == 0 {
		if st, serr := os.Stat(dst); serr == nil && st.Size() == info.Size() &&
			st.ModTime().Equal(info.ModTime()) && st.Mode().Perm() == info.Mode().Perm() {
			return nil
		}
	}
	if err := copyFile(c.Path, dst, info, d.Fsync); err != nil {
		return err
	}
	atomic.AddUint64(&d.st.Files, 1)
	return nil
}

// copyTree copies a whole subtree. It is the fallback for a rename whose source
// is not present on the target (a directory moved in from outside the tree, or
// a cross-device rename), so it has to apply the same exclusions the initial
// walk applies: otherwise a single mv of a parent directory would smuggle every
// *.tmp and cache/ below it onto the target, where a fresh run would never put
// them.
func copyTree(src, dst string, fsyncAll bool, ignore func(rel string, dir bool) bool) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		rel, rerr := filepath.Rel(src, p)
		if rerr != nil {
			return nil
		}
		if rel != "." && ignore != nil && ignore(filepath.ToSlash(rel), info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// Same rule as the initial walk and applyOne: a symlink is not
		// mirrored. Copying one would silently materialise whatever it points
		// at - possibly outside the tree, possibly a loop.
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		out := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(out, info.Mode().Perm()|0o700)
		}
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return copyFile(p, out, info, fsyncAll)
	})
}
