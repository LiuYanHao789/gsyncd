package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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
	// TargetGuard refuses to write unless the target is an existing directory
	// that is either on a non-root mount or carries the target marker. It is
	// what stops an unmounted backup disk from turning into a write into the
	// local root filesystem. Zero value is off so unit tests can use TempDir.
	TargetGuard bool
	// Verify enables transfer-time content verification: the source is hashed
	// while it is copied, the staged file is hashed again before the rename,
	// and a mismatch is retried. It is on by default in the daemon (the zero
	// value is off so unit tests do not pay for it).
	Verify bool
	// VerifyRetries is how many extra copy attempts a mismatch gets.
	VerifyRetries int
	// PreserveOwner is "auto", "true" or "false". Auto copies uid/gid only
	// when the process is root; a failure is logged once and never fails the
	// copy, because a non-root run or a root-squashed NFS target is a normal
	// deployment.
	PreserveOwner string
	// FullMode restores setuid/setgid/sticky in addition to the low nine
	// permission bits.
	FullMode bool
	// SymlinkMode is "safe", "all" or "skip".
	SymlinkMode string
	// PreserveXattrs copies extended attributes (including POSIX ACLs on
	// Linux) for regular files and directories.
	PreserveXattrs bool
	// PreserveHardlinks recreates hardlink groups inside one initial pass.
	PreserveHardlinks bool
	// Sparse preserves holes when copying regular files on platforms that
	// support SEEK_DATA/SEEK_HOLE.
	Sparse bool
	// CopySpecial recreates FIFOs and device nodes instead of skipping them.
	CopySpecial bool
	// Bandwidth is the direct copy throughput cap in bytes per second. Zero
	// means unlimited.
	Bandwidth int64

	// links maps a source hardlink group to the first target path that was
	// written for it. It is rebuilt for every initial pass and guarded because
	// copy workers run concurrently. A group whose members are processed
	// simultaneously may be recorded twice; that only costs an extra copy,
	// never a wrong link. Runtime batches copy independently.
	linkMu    sync.Mutex
	links     map[linkKey]string
	linkLocks map[linkKey]*sync.Mutex

	// limiter is rebuilt for every pass so the configured rate is shared by
	// all workers of that pass instead of being applied per file.
	limiter *rateLimiter
	// targetInfo is the identity of the target directory captured by Startup.
	// Before every batch it is compared with the current path: a target that
	// was moved, unmounted or replaced while the daemon ran must not silently
	// turn into a new directory that gsyncd then fills.
	targetInfo os.FileInfo
	// mtimeGran is the target filesystem's timestamp resolution, measured once
	// at Startup. It keeps skip/checkpoint comparisons meaningful on targets
	// that cannot store nanoseconds.
	mtimeGran time.Duration
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
	// special counts FIFOs, sockets and device nodes that were skipped. It is
	// atomic because both the walk goroutine and the copy workers see them.
	special int64
	// linksSkipped counts symlinks whose target lies outside the sync root.
	linksSkipped int64
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
		Workers: workers, Logf: logf, VerifyRetries: 2,
	}
}

// copyOptions is the per-file policy for copyFile and copyTree. It is a struct
// rather than a parameter list because the initial walk, the runtime workers
// and the rename fallbacks all share it, and each new fidelity option would
// otherwise add another positional argument to every one of them.
type copyOptions struct {
	Fsync      bool
	Verify     bool
	Retries    int
	FullMode   bool
	Chown      bool
	Xattrs     bool
	Sparse      bool
	CopySpecial bool
	Limiter     *rateLimiter
	SymlinkMode string
	Logf        func(string, string, ...any)
}

// linkKey identifies a hardlink group by device and inode.
type linkKey struct {
	dev uint64
	ino uint64
}

// beginLinkPass starts a fresh hardlink table for one initial copy.
func (d *Direct) beginLinkPass() {
	d.linkMu.Lock()
	if d.PreserveHardlinks {
		d.links = make(map[linkKey]string)
		d.linkLocks = make(map[linkKey]*sync.Mutex)
	} else {
		d.links = nil
		d.linkLocks = nil
	}
	d.linkMu.Unlock()
}

// endLinkPass releases the table.
func (d *Direct) endLinkPass() {
	d.linkMu.Lock()
	d.links = nil
	d.linkLocks = nil
	d.linkMu.Unlock()
}

// lockLink serialises the check/copy/record sequence for one hardlink group.
//
// Two workers handling two names of the same inode would otherwise both miss
// the table, copy the file twice, and never create a link. The lock is per
// group, so unrelated files still copy in parallel.
func (d *Direct) lockLink(k linkKey) func() {
	d.linkMu.Lock()
	if d.linkLocks == nil {
		d.linkLocks = make(map[linkKey]*sync.Mutex)
	}
	m := d.linkLocks[k]
	if m == nil {
		m = &sync.Mutex{}
		d.linkLocks[k] = m
	}
	d.linkMu.Unlock()
	m.Lock()
	return m.Unlock
}

func (d *Direct) lookupLink(k linkKey) (string, bool) {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	p, ok := d.links[k]
	return p, ok
}

func (d *Direct) recordLink(k linkKey, path string) {
	d.linkMu.Lock()
	if d.links == nil {
		d.links = make(map[linkKey]string)
	}
	if _, ok := d.links[k]; !ok {
		d.links[k] = path
	}
	d.linkMu.Unlock()
}

// beginBandwidth creates the limiter shared by every worker of one pass.
func (d *Direct) beginBandwidth() { d.limiter = newRateLimiter(d.Bandwidth) }

// copyOptions snapshots the backend's current policy.
func (d *Direct) copyOptions() copyOptions {
	return copyOptions{
		Fsync:       d.Fsync,
		Verify:      d.Verify,
		Retries:     d.VerifyRetries,
		FullMode:    d.FullMode,
		Chown:       d.wantOwner(),
		Xattrs:      d.PreserveXattrs,
		Sparse:      d.Sparse,
		CopySpecial: d.CopySpecial,
		Limiter:     d.limiter,
		SymlinkMode: d.SymlinkMode,
		Logf:        d.Logf,
	}
}

// wantOwner reports whether uid/gid should be copied. "auto" means "only when
// root": a non-root process cannot chown, and trying per file would turn every
// copy into a logged failure.
func (d *Direct) wantOwner() bool {
	switch d.PreserveOwner {
	case "true":
		return true
	case "false":
		return false
	default:
		return canChown()
	}
}

// dirMetaMatches reports whether the target directory already carries the
// mode and owner the source has. The checkpoint's mtime and content digest
// cannot see a chmod/chown made while the daemon was stopped, so a held
// directory is only safe when this also matches.
func (d *Direct) dirMetaMatches(target, src os.FileInfo) bool {
	modeMask := os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	if target.Mode()&modeMask != fullModeOf(src.Mode(), d.FullMode) {
		return false
	}
	if d.wantOwner() {
		su, sg, ok1 := ownerOf(src)
		tu, tg, ok2 := ownerOf(target)
		if ok1 && ok2 && (su != tu || sg != tg) {
			return false
		}
	}
	return true
}

// fullModeOf returns the permission word to restore. Basic keeps the low nine
// bits, which is what the backend did before owner and setuid support existed.
func fullModeOf(m os.FileMode, full bool) os.FileMode {
	if full {
		return m & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	}
	return m.Perm()
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
	d.beginBandwidth()
	d.beginLinkPass()
	defer d.endLinkPass()
	if d.TargetGuard {
		// Never create the target: a missing path is exactly the signal that
		// the backup filesystem is not mounted, and creating it here would
		// turn the mirror into a write into whatever directory happens to be
		// underneath.
		if err := CheckTarget(d.Target); err != nil {
			return err
		}
	} else if err := os.MkdirAll(d.Target, 0o755); err != nil {
		return err
	}
	if d.TargetGuard {
		st, err := os.Stat(d.Target)
		if err != nil {
			return err
		}
		d.targetInfo = st
	}
	d.mtimeGran = probeMtimeGranularity(d.Target)
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
		// Only entries this backend can mirror keep their target counterpart.
		// FIFOs, sockets and device nodes are skipped (copying a FIFO would
		// block forever), so an old regular file at such a name is stale and
		// has to be pruned.
		if e.IsDir() || e.Type().IsRegular() || e.Type()&os.ModeSymlink != 0 ||
			(d.CopySpecial && e.Type()&(os.ModeNamedPipe|os.ModeDevice) != 0) {
			keep[e.Name()] = struct{}{}
		}
	}
	if filepath.Clean(dstDir) == filepath.Clean(d.Target) {
		keep[TargetMarkerName] = struct{}{}
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
	src    string
	dst    string
	mtime  time.Time
	info   os.FileInfo
	opts   copyOptions
	noMeta bool
	left   int32
	sealed int32
	failed int32
	once   sync.Once
	logf   func(string, string, ...any)
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
			// This has to be checked before the root's mtime stamp as well.
			return
		}
		if p.noMeta {
			// The sync root maps to the target root, which is routinely a
			// mount point whose mode and owner belong to the operator. The
			// mtime is different: the checkpoint's root record is compared
			// against it, so it still has to be stamped.
			if err := os.Chtimes(p.dst, p.mtime, p.mtime); err != nil && p.logf != nil {
				p.logf("warn", "cannot set directory mtime on %s: %v", p.dst, err)
			}
			return
		}
		if p.info != nil {
			// Owner, mode, xattrs and mtime are applied only now: copying a
			// child updates the directory's mtime, so anything set earlier
			// would be undone by the very files this wait is for.
			applyMeta(p.src, p.dst, p.info, p.opts)
			return
		}
		// A coarse, read-only or root-squashed target filesystem lands here.
		// The directory then never matches its record and is re-verified on
		// every start - the safe direction - but it must not happen silently:
		// mtime is part of the consistency contract.
		if err := os.Chtimes(p.dst, p.mtime, p.mtime); err != nil && p.logf != nil {
			p.logf("warn", "cannot set directory mtime on %s: %v", p.dst, err)
		}
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

// noteSpecial reports a skipped FIFO/socket/device node, sampling the log the
// same way pruneDir does (issue #93).
func (d *Direct) noteSpecial(rel string, info os.FileInfo) {
	n := atomic.AddInt64(&d.special, 1)
	if n <= 10 || n%10000 == 0 {
		d.Logf("info", "skipping non-regular file %s (mode %s)", rel, info.Mode().Type())
	}
}

// noteSpecialError reports a special file that could not be recreated.
//
// copy_special is best effort: a non-root run cannot mknod a device, and a
// target filesystem may refuse FIFOs. Retrying the whole tree because of that
// would turn one unprivileged node into an endless full rescan, so the failure
// is sampled and the pass continues.
func (d *Direct) noteSpecialError(rel string, err error) {
	n := atomic.AddInt64(&d.special, 1)
	if n <= 10 || n%10000 == 0 {
		d.Logf("warn", "cannot recreate special file %s: %v (further failures suppressed)", rel, err)
	}
}

// symlinkTargetFor returns the link text to use on the target. Links that
// resolve inside the sync root are mirrored; absolute internal links are
// rewritten to the mirror root so they resolve there. Links pointing outside
// are not followed and not copied.
func (d *Direct) symlinkTargetFor(srcPath, link string) (string, bool) {
	switch d.SymlinkMode {
	case "skip":
		n := atomic.AddInt64(&d.linksSkipped, 1)
		if n <= 10 || n%10000 == 0 {
			d.Logf("info", "symlink %s not mirrored (symlink_mode=skip)", srcPath)
		}
		return "", false
	case "all":
		// Verbatim, including links that point outside the sync root. This is
		// what cp -a and rsync's default -l do, and it is the faithful choice
		// for a backup even though the target cannot necessarily resolve it.
		return link, true
	}
	text, ok := symlinkText(d.Source, d.Target, srcPath, link)
	if !ok {
		n := atomic.AddInt64(&d.linksSkipped, 1)
		if n <= 10 || n%10000 == 0 {
			d.Logf("info", "symlink %s points outside the sync root, not mirrored", srcPath)
		}
		return "", false
	}
	// A relative link keeps its text: the target tree mirrors the source
	// tree's shape, so the same relative path resolves to the mirrored file.
	return text, true
}

// writeSymlink replaces dst with a symlink to text.
func writeSymlink(dst, text string) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := createTemp(dir, tmpPrefix+"link-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_ = f.Close()
	_ = os.Remove(tmp)
	if err := os.Symlink(text, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		// Rename cannot replace a non-empty directory; that is a leftover
		// from a previous type, so remove it and retry once.
		if rmErr := os.RemoveAll(dst); rmErr == nil {
			return os.Rename(tmp, dst)
		}
		return err
	}
	return nil
}

// syncSymlink mirrors one symlink during the initial pass.
func (d *Direct) syncSymlink(rel, srcPath string, info os.FileInfo) error {
	link, err := os.Readlink(srcPath)
	if err != nil {
		return err
	}
	dst := filepath.Join(d.Target, rel)
	text, ok := d.symlinkTargetFor(srcPath, link)
	if !ok {
		if d.DeleteOnStartup {
			return os.RemoveAll(dst)
		}
		return nil
	}
	return writeSymlink(dst, text)
}

// applySymlink mirrors one symlink for a runtime event.
func (d *Direct) applySymlink(srcPath, dst string) error {
	link, err := os.Readlink(srcPath)
	if err != nil {
		return err
	}
	text, ok := d.symlinkTargetFor(srcPath, link)
	if !ok {
		if d.DeleteOnRun {
			return os.RemoveAll(dst)
		}
		return nil
	}
	return writeSymlink(dst, text)
}

// reconcileDir brings one target directory in line with its source: extra
// entries are removed and staging files left by an interrupted copy are swept.
// A missing target directory is created when mk is set.
func (d *Direct) reconcileDir(ctx context.Context, src, dst string, perm os.FileMode, mk bool, note func(error)) {
	ents, err := os.ReadDir(dst)
	if err != nil {
		if mk {
			// A file (or symlink) where a directory belongs is a type change
			// at the source that happened while the daemon was down. Remove
			// it, otherwise Mkdir fails with EEXIST and every child fails too.
			if st, lerr := os.Lstat(dst); lerr == nil && !st.IsDir() {
				note(os.RemoveAll(dst))
			}
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
	// A full target must stop the pass, not be hammered with every remaining
	// file. The cancellation is internal to this walk; the caller's context is
	// untouched.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var noSpace int64
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
		if isNoSpace(err) {
			if atomic.AddInt64(&noSpace, 1) >= 16 {
				cancel()
			}
		} else {
			atomic.StoreInt64(&noSpace, 0)
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
				select {
				case <-ctx.Done():
					if it.pd != nil {
						it.pd.fileDone(true)
					}
					continue
				default:
				}
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
	lastCommit := time.Now()

	flush := func() {
		for len(queued) > 0 {
			if len(frames) > 0 && pathWithin(queued[0].Path, frames[len(frames)-1].rel) {
				return
			}
			d.scan.Add(queued[0])
			queued = queued[1:]
			emitted++
			// Commit by record count or by time. A SIGKILL between commits
			// loses only what is still buffered; without the time bound a
			// small tree could lose its whole checkpoint (fewer than 4096
			// directories) and restart from zero.
			if emitted%scanCommitEvery == 0 || time.Since(lastCommit) >= time.Second {
				if err := d.scan.Commit(); err != nil {
					d.Logf("warn", "scan state: cannot write %s: %v", d.scan.Path(), err)
				}
				lastCommit = time.Now()
			}
		}
	}

	closeFrame := func(fr *walkFrame) error {
		if !tracking {
			// No digest to close, but the directory still has to be stamped
			// after its last file lands. Sealing with no checkpoint simply
			// waits for the copy workers and applies owner/mode/mtime.
			fr.pd.seal()
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
		mode := uint32(info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
		uid, gid, _ := ownerOf(info)
		if info.IsDir() {
			fr.hash.Dir(mode, uid, gid)
			return
		}
		if info.Mode().IsRegular() {
			fr.hash.File(info.Size(), info.ModTime().UnixNano(), mode, uid, gid)
			return
		}
		fr.hash.Other(mode, uid, gid)
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
		// Exclusions are tested before any type-specific branch. Testing them
		// after the symlink and special-file branches meant an excluded FIFO
		// or symlink was still mirrored at startup while the watcher filtered
		// it at runtime.
		if rel != "." && d.Ignore != nil && d.Ignore(filepath.ToSlash(rel), info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// Internal links are mirrored as links; links pointing outside the
			// sync root are skipped (and logged) rather than followed: the
			// target cannot resolve them and following one would smuggle
			// outside content into the mirror. The name is still folded so
			// that adding or removing a link shows up as a change.
			fold(info.Name(), info)
			note(d.syncSymlink(rel, p, info))
			return nil
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			// A FIFO/socket/device node cannot be read like a file: opening a
			// FIFO blocks until a writer appears, which would wedge a copy
			// worker (and the whole pass) forever. It can be recreated with
			// mknod/mkfifo when copy_special is on; otherwise it is skipped
			// and pruneDir removes any stale regular file at the same name.
			if d.CopySpecial {
				if err := makeSpecial(filepath.Join(d.Target, rel), info, d.copyOptions()); err != nil {
					d.noteSpecialError(rel, err)
				} else {
					atomic.AddUint64(&d.st.Files, 1)
				}
			} else {
				d.noteSpecial(rel, info)
			}
			fold(info.Name(), info)
			return nil
		}
		fold(info.Name(), info)
		dir := filepath.Join(d.Target, rel)
		if info.IsDir() {
			fr := &walkFrame{
				rel:  rel,
				src:  p,
				dst:  dir,
				perm: fullModeOf(info.Mode(), d.FullMode),
				mt:   info.ModTime().UnixNano(),
			}
			// The pending timestamp/meta work is not checkpoint specific:
			// without it a startup pass would lose directory mtime and owner
			// for every sync that does not configure a checkpoint.
			fr.pd = &pending{
				src: p, dst: dir, mtime: info.ModTime(), info: info,
				opts: d.copyOptions(), logf: d.Logf,
				// The sync root maps to the target root, which is usually a
				// mount point; stamp its mtime for the checkpoint but leave
				// mode/owner/xattrs to the operator.
				noMeta: rel == ".",
			}
			if tracking {
				fr.hash = scanstate.NewHasher()
				// The record is only usable when the target directory still
				// carries the timestamp the record expects: that is the only
				// evidence available without reading the target, and any hand
				// edit, stray write or restore changes it.
				base, ok := d.scan.Lookup(rel)
				if ok {
					checked++
				}
				// A directory can only be held when nothing about its target
				// metadata is unverified. xattrs, hardlink identity and
				// special-file contents are not in the digest, so those
				// options simply disable the shortcut; the pass re-verifies
				// instead of silently keeping stale metadata.
				holdable := !d.PreserveXattrs && !d.PreserveHardlinks && !d.CopySpecial
				if ok && holdable {
					if st, serr := os.Stat(dir); serr == nil && st.IsDir() &&
						st.ModTime().UnixNano() == truncateMtimeNanos(base.Mtime, d.mtimeGran) &&
						// The root's mode/owner are intentionally not mirrored,
						// so they must not block its checkpoint record.
						(rel == "." || d.dirMetaMatches(st, info)) {
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
		if len(frames) > 0 {
			parent := frames[len(frames)-1]
			pd = parent.pd
			if tracking && parent.hold && len(parent.todo) < maxHeldFiles {
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
	if firstErr != nil {
		return firstErr
	}
	return walkErr
}

// targetIsEmpty reports whether dir holds no entries at all.
//
// It is a heuristic, not a lock: something may write into the target a moment
// later. It is only ever used to skip work that would have been a no-op.
func targetIsEmpty(dir string) bool {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range ents {
		// The guard marker is not mirrored content; a target that holds only
		// the marker is still empty for the purposes of the fresh-target fast
		// path.
		if e.Name() == TargetMarkerName {
			continue
		}
		return false
	}
	return true
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

// sameFile reports whether two paths name the same inode.
func sameFile(a, b string) bool {
	ai, err := os.Lstat(a)
	if err != nil {
		return false
	}
	bi, err := os.Lstat(b)
	if err != nil {
		return false
	}
	return os.SameFile(ai, bi)
}

// linkInto makes dst a hardlink to src by linking to a temporary name and
// renaming it into place, so a reader never sees a partially linked path.
func linkInto(src, dst string) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := createTemp(dir, tmpPrefix+"link-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_ = f.Close()
	_ = os.Remove(tmp)
	if err := os.Link(src, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// copyFileAt copies one regular file whose parent directory the walk already
// created.
func (d *Direct) copyFileAt(ctx context.Context, rel string, info os.FileInfo) error {
	dst := filepath.Join(d.Target, rel)
	// Skip when the destination is already identical (size and mtime). On a
	// fresh target nothing can match, so the stat is skipped entirely.
	var key linkKey
	hasKey := false
	if d.PreserveHardlinks {
		if dev, ino, ok := linkKeyOf(info); ok {
			key, hasKey = linkKey{dev: dev, ino: ino}, true
		}
	}
	if hasKey {
		unlock := d.lockLink(key)
		defer unlock()
	}
	if !d.freshTarget {
		modeMask := os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
		// Lstat, and regular only: a symlink at the target path must be
		// replaced as a type change, never followed into an outside file.
		if st, err := os.Lstat(dst); err == nil && st.Mode().IsRegular() &&
			st.Size() == info.Size() && st.ModTime().UnixNano() == truncateMtimeNanos(info.ModTime().UnixNano(), d.mtimeGran) {
			modeSame := st.Mode()&modeMask == fullModeOf(info.Mode(), d.FullMode)
			ownerSame := true
			if d.wantOwner() {
				if u1, g1, ok1 := ownerOf(info); ok1 {
					if u2, g2, ok2 := ownerOf(st); ok2 && (u1 != u2 || g1 != g2) {
						ownerSame = false
					}
				}
			}
			if !modeSame || !ownerSame {
				// Content is already right; only mode/owner differ. Fix them
				// without reading and rewriting the file.
				applyMeta(d.Source+string(filepath.Separator)+rel, dst, info, d.copyOptions())
			} else if d.PreserveXattrs {
				// Only xattrs remain to check. copyXattrs compares values and
				// writes only differences, so an unchanged tree costs reads,
				// not a full metadata rewrite.
				if err := copyXattrs(d.Source+string(filepath.Separator)+rel, dst); err != nil {
					xattrWarn.Do(func() {
						warnf(d.Logf, "cannot copy extended attributes for %s (further failures suppressed): %v", dst, err)
					})
				}
			}
			if hasKey {
				// An already-populated target may hold the group members as
				// separate inodes. The fast path would otherwise just record
				// one and never link the rest, so preserve_hardlinks would
				// never converge on a restart. Replace a separate copy with a
				// link to the first one when content already matches.
				if first, ok := d.lookupLink(key); ok {
					if !sameFile(first, dst) {
						if err := linkInto(first, dst); err == nil {
							atomic.AddUint64(&d.st.Files, 1)
							return nil
						}
					}
				} else {
					d.recordLink(key, dst)
				}
			}
			return nil
		}
	}
	if hasKey {
		if first, ok := d.lookupLink(key); ok {
			if err := linkInto(first, dst); err == nil {
				atomic.AddUint64(&d.st.Files, 1)
				return nil
			}
			// Cross-device or a target that refuses links: fall back to a
			// normal copy rather than failing the file.
		}
	}
	opts := d.copyOptions()
	err := copyFile(d.Source+string(filepath.Separator)+rel, dst, info, opts)
	if errors.Is(err, os.ErrNotExist) {
		// The directory is missing despite the walk order: another sync
		// sharing this target removed it, or the walk skipped it because
		// it was unreadable. Recreate and retry once.
		if mkErr := os.MkdirAll(filepath.Dir(dst), 0o755); mkErr == nil {
			err = copyFile(d.Source+string(filepath.Separator)+rel, dst, info, opts)
		}
	}
	if err != nil {
		atomic.AddUint64(&d.st.Errors, 1)
		return err
	}
	if hasKey {
		d.recordLink(key, dst)
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
func copyBounded(dst io.Writer, src io.Reader, n int64, h hash.Hash) (int64, error) {
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
			if h != nil {
				_, _ = h.Write(buf[:r])
			}
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

// chmodWarn and chtimesWarn keep a metadata failure from flooding the log on
// a target filesystem that refuses the operation for every file. The first
// occurrence carries a path and the error; the rest are counted by the copy
// error path if they matter.
var chmodWarn, chtimesWarn, chownWarn, xattrWarn sync.Once

func warnf(logf func(string, string, ...any), format string, args ...any) {
	if logf != nil {
		logf("warn", format, args...)
	}
}

// ErrVerifyMismatch marks a staged copy whose bytes differ from the source.
var ErrVerifyMismatch = errors.New("content verification mismatch")

// copyFile writes to a temporary file in the destination directory and renames
// it into place, so a reader on the target never sees a half written file.
// fsyncAll forces the flush for small files too.
//
// With verify set, the source is hashed while it is copied and the staged file
// is read back and hashed before the rename. A mismatch retries from scratch;
// the previously correct target is untouched until the rename, so a failed
// verification never destroys good data.
func copyFile(src, dst string, info os.FileInfo, opts copyOptions) error {
	if opts.Retries < 0 {
		opts.Retries = 0
	}
	var last error
	for attempt := 0; attempt <= opts.Retries; attempt++ {
		err := copyFileOnce(src, dst, info, opts)
		if err == nil {
			return nil
		}
		last = err
		if !opts.Verify || !errors.Is(err, ErrVerifyMismatch) {
			return err
		}
		warnf(opts.Logf, "content verification failed for %s (attempt %d/%d): %v", dst, attempt+1, opts.Retries+1, err)
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	return last
}

func copyFileOnce(src, dst string, info os.FileInfo, opts copyOptions) error {
	in, err := openRead(src)
	if err != nil {
		return err
	}
	defer in.Close()

	var srcHash hash.Hash
	if opts.Verify {
		srcHash = sha256.New()
	}

	dir := filepath.Dir(dst)
	tmp, err := createTemp(dir, tmpPrefix+"*")
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
	copyNormal := func() error {
		var r io.Reader = in
		if opts.Limiter != nil {
			r = newThrottledReader(r, opts.Limiter)
		}
		_, err := copyBounded(tmp, r, info.Size(), srcHash)
		return err
	}
	var copyErr error
	if opts.Sparse && sparseSupported() && sparseSupportedOn(in) {
		// The sparse copier hashes the full logical content, including holes,
		// so verification still compares what a reader would see.
		_, copyErr = copySparse(tmp, in, info.Size(), srcHash, opts.Limiter)
		if errors.Is(copyErr, errSparseUnsupported) {
			// A filesystem can accept SEEK_DATA and still reject SEEK_HOLE, or
			// change its answer mid-file. Restart as a plain copy with a clean
			// hash and an empty staging file instead of failing the transfer.
			if opts.Verify {
				srcHash = sha256.New()
			}
			if _, err := in.Seek(0, io.SeekStart); err != nil {
				tmp.Close()
				return err
			}
			if err := tmp.Truncate(0); err != nil {
				tmp.Close()
				return err
			}
			if _, err := tmp.Seek(0, io.SeekStart); err != nil {
				tmp.Close()
				return err
			}
			copyErr = copyNormal()
		}
	} else {
		copyErr = copyNormal()
	}
	if copyErr != nil {
		tmp.Close()
		return copyErr
	}
	if opts.Fsync || info.Size() >= fsyncThreshold {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if opts.Verify {
		if err := verifyStaged(tmpName, info.Size(), srcHash); err != nil {
			return err
		}
	}
	// chown before chmod: on many systems chown clears setuid/setgid, so
	// setting the mode first would silently drop the bits the operator asked
	// to preserve.
	if opts.Chown {
		if uid, gid, ok := ownerOf(info); ok {
			if err := chownPath(tmpName, uid, gid); err != nil {
				chownWarn.Do(func() {
					warnf(opts.Logf, "cannot set owner on %s (further failures suppressed): %v", tmpName, err)
				})
			}
		}
	}
	// Xattrs before the final chmod: setting user.* on a read-only inode
	// fails with EACCES, while the staging file is still 0600 here. The
	// final chmod also settles the POSIX ACL mask at the source mode.
	if opts.Xattrs {
		if err := copyXattrs(src, tmpName); err != nil {
			xattrWarn.Do(func() {
				warnf(opts.Logf, "cannot copy extended attributes for %s (further failures suppressed): %v", src, err)
			})
		}
	}
	if err := os.Chmod(tmpName, fullModeOf(info.Mode(), opts.FullMode)); err != nil {
		chmodWarn.Do(func() { warnf(opts.Logf, "cannot set permissions on %s: %v", tmpName, err) })
	}
	if err := os.Chtimes(tmpName, info.ModTime(), info.ModTime()); err != nil {
		// mtime is part of the consistency contract, so a filesystem that
		// refuses it (root-squashed NFS, some FUSE targets) must be visible.
		// The copy itself is still correct; verification reports the mismatch.
		chtimesWarn.Do(func() { warnf(opts.Logf, "cannot set mtime on %s: %v", tmpName, err) })
	}
	// A directory where a file belongs is a type change; rename(2) cannot
	// replace it, and leaving it would make every later copy of this path
	// fail. Only directories are removed here, and only for this exact path.
	if st, err := os.Lstat(dst); err == nil && st.IsDir() {
		_ = os.RemoveAll(dst)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		// Windows refuses to rename over an existing file in some cases. On
		// POSIX/NFS a failed rename must not be retried after unlinking the
		// destination: that would turn a transient error (ESTALE/EIO) into
		// the loss of the previously correct target file.
		if runtime.GOOS == "windows" {
			if rmErr := os.Remove(dst); rmErr == nil {
				err = os.Rename(tmpName, dst)
			}
		}
		if err != nil {
			return err
		}
	}
	tmpName = ""
	return nil
}

// verifyStaged reads the staged file back and compares its hash with the
// source's. Reading through the same mount may be served by the client cache;
// that verifies what readers of this mount will see, not necessarily the
// server's stable storage.
func verifyStaged(path string, size int64, want hash.Hash) error {
	f, err := openRead(path)
	if err != nil {
		return err
	}
	defer f.Close()
	got := sha256.New()
	if _, err := copyBounded(io.Discard, f, size, got); err != nil {
		return err
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		return fmt.Errorf("%w: %s", ErrVerifyMismatch, path)
	}
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
	// Runtime batches do not preserve hardlinks; clear any table left by an
	// interrupted initial pass so stale inode keys can never be consulted.
	d.endLinkPass()
	d.beginBandwidth()
	// The target was verified by Startup. Re-check its identity before a
	// batch: if the path disappeared (a moved mount, an unmounted disk whose
	// mount point is now an ordinary directory) writing would recreate it and
	// fill whatever is underneath. os.SameFile compares device and inode, so a
	// remount or replacement is caught without reading mountinfo per batch.
	if d.TargetGuard && d.targetInfo != nil {
		st, err := os.Stat(d.Target)
		if err != nil {
			return fmt.Errorf("target %s disappeared while running, refusing to write: %w", d.Target, err)
		}
		if !os.SameFile(d.targetInfo, st) {
			return fmt.Errorf("target %s is no longer the filesystem verified at startup, refusing to write", d.Target)
		}
	}

	// Renames first: they are cheap and free the old name before a possible
	// copy of a file with the same path. Directory changes run next and
	// sequentially, so a directory recreate/removal finishes before any file
	// under it is written; otherwise a worker could copy a new file and a
	// concurrent RemoveAll could delete it again.
	var renames, dirs, rest []aggregate.Change
	for _, c := range b.Changes {
		switch {
		case c.From != "" && c.Op&event.Delete == 0:
			renames = append(renames, c)
		case c.Kind == aggregate.KindDir:
			dirs = append(dirs, c)
		default:
			rest = append(rest, c)
		}
	}
	// Ancestor renames must run before renames of their descendants: a file
	// renamed inside a directory that is itself renamed is retargeted to the
	// new directory, which has to exist on the target first.
	sort.SliceStable(renames, func(i, j int) bool {
		di, dj := pathDepth(renames[i].From), pathDepth(renames[j].From)
		if di != dj {
			return di < dj
		}
		return renames[i].From < renames[j].From
	})

	// Directories whose metadata has to be restored after the batch. Copying
	// a file changes its parent's mtime, so the parent can only be stamped
	// once every worker is done. These are source paths (c.Path/c.From), and
	// the source filesystem has already applied the rename even though the
	// target has not, so the captured FileInfo describes the moved tree, not
	// the old target copy.
	dirMeta := make(map[string]os.FileInfo)
	noteDir := func(p string) {
		if _, ok := dirMeta[p]; ok {
			return
		}
		if info, err := os.Lstat(p); err == nil && info.IsDir() {
			dirMeta[p] = info
		}
	}
	for _, c := range renames {
		noteDir(c.Path)
		noteDir(filepath.Dir(c.Path))
		noteDir(filepath.Dir(c.From))
	}
	for _, c := range dirs {
		noteDir(c.Path)
		noteDir(filepath.Dir(c.Path))
	}
	for _, c := range rest {
		noteDir(filepath.Dir(c.Path))
	}
	// A full target stops the batch after a few failures instead of trying
	// every remaining change.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var noSpace int64
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
		if isNoSpace(err) {
			if atomic.AddInt64(&noSpace, 1) >= 16 {
				cancel()
			}
		} else {
			atomic.StoreInt64(&noSpace, 0)
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
	// Directory operations are ordered by depth (ancestors first) and deletes
	// before creates at the same depth. That is what makes "delete a directory
	// and recreate it" deterministic instead of racing a descendant copy.
	sort.SliceStable(dirs, func(i, j int) bool {
		di, dj := pathDepth(dirs[i].Path), pathDepth(dirs[j].Path)
		if di != dj {
			return di < dj
		}
		idel := dirs[i].Op&event.Delete != 0 && dirs[i].Op&(event.Create|event.Write) == 0
		jdel := dirs[j].Op&event.Delete != 0 && dirs[j].Op&(event.Create|event.Write) == 0
		if idel != jdel {
			return idel
		}
		return dirs[i].Path < dirs[j].Path
	})
	for _, c := range dirs {
		select {
		case <-ctx.Done():
			note(ctx.Err())
			continue
		default:
		}
		if err := d.applyOne(ctx, c); err != nil {
			d.Logf("debug", "apply dir %s: %v", c.Path, err)
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

	// Restore directory metadata last, after every file is in place. This is
	// what makes a runtime file change stop leaving the parent directory with
	// a fresh mtime (and therefore a checkpoint miss on the next start).
	// Only stamp directories when the whole batch succeeded. A partially
	// failed batch is re-added and retried; stamping a directory whose file is
	// missing would make it look more complete than it is.
	if firstErr == nil {
		opts := d.copyOptions()
		// Deepest first: a directory whose final mode removes execute or
		// write access must be stamped after everything below it has been.
		type dirWork struct {
			src   string
			info  os.FileInfo
			depth int
		}
		list := make([]dirWork, 0, len(dirMeta))
		for src, info := range dirMeta {
			list = append(list, dirWork{src: src, info: info, depth: strings.Count(filepath.ToSlash(src), "/")})
		}
		sort.SliceStable(list, func(i, j int) bool { return list[i].depth > list[j].depth })
		for _, w := range list {
			if filepath.Clean(w.src) == filepath.Clean(d.Source) {
				// Never chmod or chown the target root itself. It is routinely
				// a mount point owned by the operator, and changing its mode
				// can make the mount unusable for everyone else.
				continue
			}
			rel, ok := relInSource(d.Source, w.src)
			if !ok {
				continue
			}
			applyMeta(w.src, filepath.Join(d.Target, rel), w.info, opts)
		}
	}

	if skipped > 0 {
		d.Logf("warn", "%d changes not applied: %v", skipped, ctx.Err())
	}
	if firstErr != nil {
		return firstErr
	}
	if skipped > 0 {
		return ctx.Err()
	}
	return nil
}

// relInside resolves a change path against the source and refuses anything
// outside it. Every path a backend touches is joined onto the target, so a path
// that escapes the source would make filepath.Join produce something outside the
// destination — writing or deleting data the operator never pointed at. Events
// are routed before they get here; this is the belt to that suspenders.
// pathDepth counts path components; used to apply ancestors before
// descendants.
func pathDepth(p string) int {
	return strings.Count(filepath.ToSlash(p), "/")
}

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
	d.Logf("debug", "rename %s -> %s prev=%v op=%s", c.From, c.Path, c.PrevFrom, c.Op)
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
	// A pending write was folded into this move (aggregate.applyMove carries
	// Write/Create through a rename). Renaming the old target file would move
	// the stale copy to the new name, so the content has to be copied.
	// A rename that also carries a content or metadata change cannot be
	// served by renaming the old target copy: that would move the stale file
	// (or its stale mode/owner) to the new name. Attrib counts because mode
	// and mtime may have changed without a Write.
	dirty := c.Op&(event.Create|event.Write|event.Attrib) != 0
	if !dirty {
		if err := os.Rename(srcDst, dstDst); err == nil {
			d.removePrevFrom(c)
			atomic.AddUint64(&d.st.Files, 1)
			return nil
		}
	}
	// Cross-device, missing source on the target, or a dirty move: copy+remove.
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
			// A rename replaces its destination. copyTree merges into an
			// existing directory, so anything already at the new name has to
			// go first or entries the source no longer has would survive.
			if err := os.RemoveAll(dstDst); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := copyTree(c.Path, dstDst, d.Source, d.Target, d.copyOptions(), d.symlinkTargetFor, ign); err != nil {
				return err
			}
		} else if info.Mode()&os.ModeSymlink != 0 {
			if err := d.applySymlink(c.Path, dstDst); err != nil {
				return err
			}
		} else if !info.Mode().IsRegular() {
			if d.CopySpecial {
				if err := makeSpecial(dstDst, info, d.copyOptions()); err != nil {
					d.noteSpecialError(relTo, err)
					return nil
				}
			} else {
				d.noteSpecial(relTo, info)
			}
		} else if err := copyFile(c.Path, dstDst, info, d.copyOptions()); err != nil {
			return err
		}
	} else if dirty {
		// The source vanished after the rename event; the destination must not
		// keep an old copy under the new name.
		_ = os.RemoveAll(dstDst)
	}
	_ = os.RemoveAll(srcDst)
	d.removePrevFrom(c)
	atomic.AddUint64(&d.st.Files, 1)
	return nil
}

// removePrevFrom deletes the target copies of earlier rename sources folded
// into this change. They have no event of their own.
func (d *Direct) removePrevFrom(c aggregate.Change) {
	for _, pf := range c.PrevFrom {
		if pf == c.Path || pf == c.From {
			continue
		}
		if rel, err := d.relInside(pf); err == nil {
			_ = os.RemoveAll(filepath.Join(d.Target, rel))
		}
	}
}

func (d *Direct) applyOne(ctx context.Context, c aggregate.Change) error {
	rel, err := d.relInside(c.Path)
	if err != nil {
		return skipOutside(d.Logf, err)
	}
	dst := filepath.Join(d.Target, rel)

	if c.Op&event.Delete != 0 && c.Op&event.Create == 0 {
		d.Logf("debug", "delete %s from=%s prev=%v op=%s", c.Path, c.From, c.PrevFrom, c.Op)
		if d.DeleteOnRun {
			if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			atomic.AddUint64(&d.st.Deletes, 1)
		}
		// The old name of a rename is not a runtime deletion, even when the
		// new name was deleted again in the same window (mv a b && rm b).
		if c.From != "" && c.Op&event.Move != 0 {
			// If the old name was recreated at the source after the move, its
			// target copy must stay.
			if _, statErr := os.Lstat(c.From); errors.Is(statErr, os.ErrNotExist) {
				if relFrom, err := d.relInside(c.From); err == nil {
					if err := os.RemoveAll(filepath.Join(d.Target, relFrom)); err != nil && !errors.Is(err, os.ErrNotExist) {
						return err
					}
					atomic.AddUint64(&d.st.Deletes, 1)
				}
			}
		}
		d.removePrevFrom(c)
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
	// A path deleted and recreated in the same window must not keep anything
	// the source removed, for every type: dir -> file, file -> dir, or a
	// symlink replacing a regular file. This is the one case where a create
	// carries an implied removal.
	if c.Recreate && d.DeleteOnRun {
		if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return d.applySymlink(c.Path, dst)
	}
	if info.IsDir() {
		if c.Subtree {
			// A rename split at a filter boundary: the kernel said "moved
			// here", but the source side was excluded, so the destination has
			// to be built from the source subtree instead of by renaming the
			// old target copy. Any pre-existing destination is removed first:
			// a rename replaces its destination, and whatever is there is not
			// part of the moved tree.
			if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			var ign func(rel string, dir bool) bool
			if d.Ignore != nil {
				// copyTree hands out paths relative to the subtree it walks;
				// rebase them to the sync root so the exclude rules apply.
				// relInside already proved c.Path is under the root.
				base, _ := relInSource(d.Source, c.Path)
				ign = func(rel string, dir bool) bool {
					return d.Ignore(filepath.ToSlash(filepath.Join(base, rel)), dir)
				}
			}
			if err := copyTree(c.Path, dst, d.Source, d.Target, d.copyOptions(), d.symlinkTargetFor, ign); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// Renamed again after the window was taken; the follow-up
					// move event carries the new name.
					return nil
				}
				return err
			}
			atomic.AddUint64(&d.st.Files, 1)
			return nil
		}
		// A file or symlink where a directory belongs is a type change.
		// MkdirAll would follow a symlink to a directory on some systems.
		if st, lerr := os.Lstat(dst); lerr == nil && !st.IsDir() {
			if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := os.MkdirAll(dst, fullModeOf(info.Mode(), d.FullMode)|0o700); err != nil {
			return err
		}
		// Owner/mode/mtime are applied by the batch-level pass after all files
		// under it have landed; setting them here would be undone by the very
		// children this batch is about to write.
		return nil
	}
	if !info.Mode().IsRegular() {
		if d.CopySpecial {
			if err := makeSpecial(dst, info, d.copyOptions()); err != nil {
				d.noteSpecialError(rel, err)
				return nil
			}
			atomic.AddUint64(&d.st.Files, 1)
			return nil
		}
		d.noteSpecial(rel, info)
		if d.DeleteOnRun {
			return os.RemoveAll(dst)
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
		// Lstat, and regular only: applyMeta follows symlinks, so a
		// symlink at the target path must fall through to copyFile and be
		// replaced instead of having its referent modified.
		if st, serr := os.Lstat(dst); serr == nil && st.Mode().IsRegular() &&
			st.Size() == info.Size() &&
			st.ModTime().UnixNano() == truncateMtimeNanos(info.ModTime().UnixNano(), d.mtimeGran) {
			// A metadata-only event (chmod/chown/xattr) whose content is
			// already correct. Apply the metadata without recopying bytes;
			// mode, owner and xattrs are all handled by applyMeta.
			applyMeta(c.Path, dst, info, d.copyOptions())
			return nil
		}
	}
	if err := copyFile(c.Path, dst, info, d.copyOptions()); err != nil {
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
func copyTree(src, dst, srcRoot, dstRoot string, opts copyOptions, linkFn func(string, string) (string, bool), ignore func(rel string, dir bool) bool) error {
	// Walk starts with a callback whose error this function ignores (a
	// subtree may legitimately vanish mid-scan), so a missing source root
	// would otherwise be a successful no-op after the caller removed the
	// destination.
	if _, err := os.Lstat(src); err != nil {
		return err
	}
	// Directories are stamped after their contents: copying a child updates
	// the parent's mtime, so setting it during the walk would be undone by the
	// next file. Walk is pre-order, hence the reverse pass.
	type dirMeta struct {
		src  string
		dst  string
		info os.FileInfo
	}
	var dirs []dirMeta
	walkErr := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			// A subtree that vanished mid-copy is harmless; a permission or
			// I/O error is not. The caller removed the old destination, so
			// reporting success here would leave a silently incomplete
			// mirror with no event left to repair it.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
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
		out := filepath.Join(dst, rel)
		if info.Mode()&os.ModeSymlink != 0 {
			link, lerr := os.Readlink(p)
			if lerr != nil {
				return nil
			}
			var text string
			var ok bool
			if linkFn != nil {
				text, ok = linkFn(p, link)
			} else {
				switch opts.SymlinkMode {
				case "skip":
					ok = false
				case "all":
					text, ok = link, true
				default:
					text, ok = symlinkText(srcRoot, dstRoot, p, link)
				}
			}
			if !ok {
				if opts.Logf != nil {
					opts.Logf("info", "symlink %s not mirrored (mode %s)", p, opts.SymlinkMode)
				}
				return nil
			}
			return writeSymlink(out, text)
		}
		if info.IsDir() {
			if st, lerr := os.Lstat(out); lerr == nil && !st.IsDir() {
				if err := os.RemoveAll(out); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if err := os.MkdirAll(out, fullModeOf(info.Mode(), opts.FullMode)|0o700); err != nil {
				return err
			}
			dirs = append(dirs, dirMeta{src: p, dst: out, info: info})
			return nil
		}
		if !info.Mode().IsRegular() {
			if opts.CopySpecial {
				if err := makeSpecial(out, info, opts); err != nil && opts.Logf != nil {
					opts.Logf("warn", "cannot recreate special file %s: %v (further failures suppressed)", p, err)
				}
				return nil
			}
			if opts.Logf != nil {
				opts.Logf("info", "skipping non-regular file %s (mode %s)", p, info.Mode().Type())
			}
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return copyFile(p, out, info, opts)
	})
	if walkErr != nil {
		return walkErr
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		applyMeta(dirs[i].src, dirs[i].dst, dirs[i].info, opts)
	}
	return nil
}

// applyMeta restores owner, mode, xattrs and mtime on one path. It is shared
// by files and directories so the order (chown clears setuid, ACLs are set
// after chmod) exists once.
//
// It is deliberately best effort: a filesystem that refuses chown or xattr
// must not turn an otherwise complete copy into a failed batch. The first
// failure of each kind is logged by a warn-once guard.
func applyMeta(src, dst string, info os.FileInfo, opts copyOptions) {
	if opts.Chown {
		if uid, gid, ok := ownerOf(info); ok {
			if err := chownPath(dst, uid, gid); err != nil {
				chownWarn.Do(func() {
					warnf(opts.Logf, "cannot set owner on %s (further failures suppressed): %v", dst, err)
				})
			}
		}
	}
	if opts.Xattrs {
		// Make the inode writable for setxattr if its current mode is not;
		// the final chmod below restores the source mode regardless.
		if st, err := os.Stat(dst); err == nil && st.Mode().Perm()&0o200 == 0 {
			_ = os.Chmod(dst, st.Mode()|0o200)
		}
		if err := copyXattrs(src, dst); err != nil {
			xattrWarn.Do(func() {
				warnf(opts.Logf, "cannot copy extended attributes for %s (further failures suppressed): %v", dst, err)
			})
		}
	}
	if err := os.Chmod(dst, fullModeOf(info.Mode(), opts.FullMode)); err != nil {
		chmodWarn.Do(func() {
			warnf(opts.Logf, "cannot set permissions on %s (further failures suppressed): %v", dst, err)
		})
	}
	if err := os.Chtimes(dst, info.ModTime(), info.ModTime()); err != nil {
		chtimesWarn.Do(func() {
			warnf(opts.Logf, "cannot set mtime on %s (further failures suppressed): %v", dst, err)
		})
	}
}


// symlinkText returns the link text to write on the target. Links resolving
// inside srcRoot are mirrored (absolute ones rewritten under dstRoot); links
// pointing outside are reported as not mirrorable.
func symlinkText(srcRoot, dstRoot, srcPath, link string) (string, bool) {
	var resolved string
	if filepath.IsAbs(link) {
		resolved = filepath.Clean(link)
	} else {
		resolved = filepath.Clean(filepath.Join(filepath.Dir(srcPath), link))
	}
	root := filepath.Clean(srcRoot)
	if resolved == root {
		return filepath.Clean(dstRoot), true
	}
	rel, ok := relInSource(root, resolved)
	if !ok {
		return "", false
	}
	if filepath.IsAbs(link) {
		return filepath.Join(dstRoot, rel), true
	}
	return link, true
}
