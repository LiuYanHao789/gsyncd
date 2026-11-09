package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// RSync pushes changes with an external rsync binary.
//
// Two shapes exist:
//
//	rsync   : source dir -> local dir or user@host:dir
//	rsyncssh: source dir -> user@host, with --rsh=ssh
//
// The change list is streamed over stdin with --files-from=- --from0 instead of
// being placed on the command line. That removes the ARG_MAX ceiling that bit
// the original lsyncd (it silently truncated large batches) and lets a single
// batch carry hundreds of thousands of paths.
type RSync struct {
	Source string
	Target string
	// TargetDir is only used by rsyncssh, where Target is a bare host.
	TargetDir string
	// SSH indicates the rsyncssh dialect.
	SSH     bool
	SSHOpts SSHOptions
	// Extra arguments appended verbatim before the source/target pair.
	Extra []string
	// Delete enables deletion on the target at all.
	Delete bool
	// DeleteOnStartup adds --delete to the initial whole-tree transfer; it is
	// off for delete = "running", where the target is a deliberate superset.
	DeleteOnStartup bool
	// DeleteOnRun propagates deletes seen while running; it is off for
	// delete = "startup".
	DeleteOnRun bool
	// BatchLimit splits large windows into several invocations.
	BatchLimit int
	// TargetGuard refuses to write unless a local target already exists and
	// is on a real mount or carries the marker. It is skipped for rsyncssh,
	// where Target names a host rather than a path.
	TargetGuard bool
	// targetInfo is the identity captured at Startup; Apply re-checks it so a
	// target moved or replaced at runtime is not recreated and filled.
	targetInfo os.FileInfo
	// Excludes are rsync style patterns. They are forwarded to rsync as
	// --exclude/--include so that the initial whole-tree transfer honours them
	// too: filtering only the event stream would leave every excluded file
	// copied once at startup.
	Excludes []string
	// dropWarned stops the "deletions not mirrored" notice from repeating for
	// every batch; the condition is a configuration choice, not an event.
	dropWarned bool
	// partialAt rate limits the exit-23 notice. A permanently unreadable file
	// (a broken mount, a revoked permission) makes every batch report it, and
	// an unthrottled warning on a 10 TB tree is issue #93 all over again.
	partialMu sync.Mutex
	partialAt time.Time

	// Timeout bounds a single invocation.
	Timeout time.Duration
	Logf    func(level string, format string, args ...any)
	OnLine  func(line string)

	st Stats
}

// SSHOptions configures the ssh transport for rsyncssh mode.
type SSHOptions struct {
	Binary       string
	Port         int
	IdentityFile string
	Extra        []string
}

// NewRSync builds a local/remote rsync backend.
func NewRSync(source, target string, del bool, extra []string, logf func(string, string, ...any)) *RSync {
	if logf == nil {
		logf = func(string, string, ...any) {}
	}
	return &RSync{
		Source: source, Target: target,
		Delete: del, DeleteOnStartup: del, DeleteOnRun: del,
		Extra: extra, BatchLimit: 10_000, Logf: logf,
	}
}

// Name implements Backend.
func (r *RSync) Name() string {
	if r.SSH {
		return "rsyncssh"
	}
	return "rsync"
}

// Stats returns a snapshot.
//
// The counters are updated with atomic adds from concurrent Apply calls, so
// they have to be read back the same way: a plain struct copy is a torn read on
// 32 bit platforms and the race detector (where it works) flags it.
func (r *RSync) Stats() Stats {
	return Stats{
		Batches: atomic.LoadUint64(&r.st.Batches),
		Files:   atomic.LoadUint64(&r.st.Files),
		Deletes: atomic.LoadUint64(&r.st.Deletes),
		Errors:  atomic.LoadUint64(&r.st.Errors),
		Retries: atomic.LoadUint64(&r.st.Retries),
	}
}

// Close implements Backend.
func (r *RSync) Close() error { return nil }

func (r *RSync) binary() string {
	if b := os.Getenv("GSYNCD_RSYNC"); b != "" {
		return b
	}
	return "rsync"
}

// baseArgs builds the arguments shared by every invocation.
func (r *RSync) baseArgs() []string {
	// -r is explicit on purpose. rsync's own manual says the --files-from
	// exception is the one case where -a does not imply -r:
	//
	//	"The only exception to the above equivalence is when --files-from is
	//	 specified, in which case -r is not implied."
	//
	// Without it an incremental batch would create the listed directories but
	// never descend into them, so a directory that appeared at runtime (and
	// whose children are represented only by that single path) would arrive
	// empty. The original lsyncd passed -r here as well.
	args := []string{"-a", "-r", "--from0", "--files-from=-"}
	// --safe-links makes rsync ignore symlinks whose target escapes the copied
	// tree, matching the direct backend's "internal links only" rule.
	args = append(args, "--safe-links", "--exclude="+TargetMarkerName)
	// Keep the transfer restartable after an interruption: a 100 TB tree must
	// not begin again from zero because one batch failed.
	args = append(args, "--partial", "--partial-dir=.rsync-partial")
	// --delete is deliberately absent here: an incremental run carries only the
	// changed paths, and rsync would then consider everything else extraneous
	// (see deleteList). Only Startup, which mirrors the whole tree, adds it.
	if r.SSH {
		args = append(args, r.rshArg())
	}
	args = append(args, r.Extra...)
	args = append(args, r.excludeArgs()...)
	return args
}

// excludeArgs renders the exclude list for rsync.
//
// rsync stops at the first matching rule while the event filter here keeps the
// last one, so the list is reversed: that makes "later pattern wins" hold in
// both places. A leading "!" is the filter's include marker and becomes
// --include, which is the closest rsync equivalent.
func (r *RSync) excludeArgs() []string {
	if len(r.Excludes) == 0 {
		return nil
	}
	out := make([]string, 0, len(r.Excludes))
	for i := len(r.Excludes) - 1; i >= 0; i-- {
		p := strings.TrimSpace(r.Excludes[i])
		if p == "" || strings.HasPrefix(p, "#") {
			continue
		}
		if strings.HasPrefix(p, "!") {
			out = append(out, "--include="+strings.TrimPrefix(p, "!"))
			continue
		}
		out = append(out, "--exclude="+p)
	}
	return out
}

// rshArg renders the --rsh= argument for rsyncssh mode.
func (r *RSync) rshArg() string {
	if !r.SSH {
		return ""
	}
	rsh := "ssh"
	if r.SSHOpts.Binary != "" {
		rsh = r.SSHOpts.Binary
	}
	if r.SSHOpts.Port > 0 {
		rsh += " -p " + strconv.Itoa(r.SSHOpts.Port)
	}
	if r.SSHOpts.IdentityFile != "" {
		rsh += " -i " + r.SSHOpts.IdentityFile
	}
	rsh += " -o BatchMode=yes"
	for _, o := range r.SSHOpts.Extra {
		rsh += " " + o
	}
	return "--rsh=" + rsh
}

// withoutDeleteFlags strips --delete* from user supplied extra arguments.
// They are only meaningful for a whole-tree transfer; on an incremental
// --files-from run they would delete everything not in the batch, which on a
// 100 TB mirror is the difference between "removed three files" and "wiped the
// destination".
func withoutDeleteFlags(args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		// --del is rsync's alias for --delete-during; -d is --dirs and is
		// unrelated, so it stays.
		if strings.HasPrefix(a, "--delete") || a == "--del" {
			continue
		}
		out = append(out, a)
	}
	return out
}

// dest returns the rsync destination operand.
func (r *RSync) dest() string {
	if r.SSH {
		d := r.Target
		dir := r.TargetDir
		if dir == "" {
			dir = r.Source
		}
		return d + ":" + dir
	}
	return r.Target
}

// Startup performs the initial transfer of the whole tree.
func (r *RSync) Startup(ctx context.Context) error {
	if r.TargetGuard && !r.SSH && !strings.Contains(r.Target, ":") {
		if err := CheckTarget(r.Target); err != nil {
			return err
		}
		st, err := os.Stat(r.Target)
		if err != nil {
			return err
		}
		r.targetInfo = st
	}
	src := strings.TrimSuffix(r.Source, string(filepath.Separator)) + string(filepath.Separator)
	args := r.baseArgs()
	// A startup run must mirror the tree, so it does not use --files-from.
	filtered := make([]string, 0, len(args))
	for _, a := range args {
		if strings.HasPrefix(a, "--files-from") || a == "--from0" {
			continue
		}
		filtered = append(filtered, a)
	}
	if r.Delete && r.DeleteOnStartup {
		filtered = append(filtered, "--delete")
	}
	filtered = append(filtered, src, r.dest())
	if err := r.run(ctx, filtered, nil); err != nil {
		return err
	}
	if r.Delete && r.DeleteOnStartup && !r.SSH {
		if err := r.removeExtras(ctx); err != nil {
			r.Logf("warn", "prune: %v", err)
		}
	}
	return nil
}

// removeExtras deletes target entries missing on the source. rsync --delete
// already covers files, but empty directories left behind by a crash are not
// always cleaned, and on a 100 TB mirror those add up.
func (r *RSync) removeExtras(ctx context.Context) error {
	if _, err := os.Stat(r.Target); err != nil {
		return nil
	}
	// WalkDir instead of Walk: the directory entry already says whether the
	// subject is a directory, so the per-entry Lstat of the whole destination
	// tree disappears. On a 100 TB mirror that is a hundred million syscalls
	// saved for a pass that only exists to clean up empty directories.
	return filepath.WalkDir(r.Target, func(p string, d os.DirEntry, err error) error {
		if err != nil || p == r.Target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !d.IsDir() {
			// Files are rsync's job: --delete removed them already.
			return nil
		}
		rel, rerr := filepath.Rel(r.Target, p)
		if rerr != nil {
			return nil
		}
		if _, serr := os.Lstat(filepath.Join(r.Source, rel)); serr != nil {
			// A directory that still holds files is either a race with rsync
			// or something we must not touch, and os.Remove reports that as
			// ENOTEMPTY. Returning the error here would abort the whole walk
			// on the first non-empty leftover, so the remaining orphans on a
			// 100 TB mirror would never be visited. Ignore it and continue.
			_ = os.Remove(p)
		}
		return nil
	})
}

// Apply implements Backend by streaming the changed relative paths to rsync.
func (r *RSync) Apply(ctx context.Context, b Batch) error {
	if b.Full {
		return r.Startup(ctx)
	}
	if len(b.Changes) == 0 {
		return nil
	}
	if r.TargetGuard && !r.SSH && !strings.Contains(r.Target, ":") && r.targetInfo != nil {
		st, err := os.Stat(r.Target)
		if err != nil {
			return fmt.Errorf("target %s disappeared while running, refusing to write: %w", r.Target, err)
		}
		if !os.SameFile(r.targetInfo, st) {
			return fmt.Errorf("target %s is no longer the filesystem verified at startup, refusing to write", r.Target)
		}
	}
	atomic.AddUint64(&r.st.Batches, 1)

	// A directory deleted and recreated in one window must lose its old target
	// contents before the new contents are uploaded. rsync's incremental path
	// has no --delete, so that removal is explicit and happens first.
	if r.DeleteOnRun {
		for _, c := range b.Changes {
			if !c.Recreate {
				continue
			}
			rel, ok := relInSource(r.Source, c.Path)
			if !ok {
				continue
			}
			if r.SSH {
				if err := r.deleteList(ctx, []string{c.Path}); err != nil {
					return err
				}
				continue
			}
			if err := os.RemoveAll(filepath.Join(r.Target, rel)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	uploads, deletes := r.plan(b.Changes)
	// Exit 23 means some paths could not be read or transferred. It is not
	// fatal to the invocation (the rest of the batch did land), but the batch
	// must be reported so the engine re-adds those paths: silently treating it
	// as delivered leaves them missing until a full resync.
	var partial error

	if len(uploads) > 0 {
		limit := r.BatchLimit
		if limit <= 0 {
			limit = 10_000
		}
		for start := 0; start < len(uploads); start += limit {
			end := start + limit
			if end > len(uploads) {
				end = len(uploads)
			}
			err := r.upload(ctx, uploads[start:end])
			if err != nil {
				if !r.benignExit(err) {
					atomic.AddUint64(&r.st.Errors, 1)
					return err
				}
				if ExitCode(err) == 23 && partial == nil {
					partial = err
				}
			}
			// A partial transfer still moved the paths that survived, so they
			// are counted: the counter is "what reached the target", and
			// undercounting it during a mass delete makes progress look stuck.
			atomic.AddUint64(&r.st.Files, uint64(end-start))
		}
	}
	if len(deletes) > 0 {
		if r.DeleteOnRun {
			if err := r.deleteList(ctx, deletes); err != nil {
				if r.benignExit(err) {
					if ExitCode(err) == 23 && partial == nil {
						partial = err
					}
				} else {
					atomic.AddUint64(&r.st.Errors, 1)
					return err
				}
			}
			atomic.AddUint64(&r.st.Deletes, uint64(len(deletes)))
		} else if !r.dropWarned {
			// delete = "startup" means runtime removals are intentionally not
			// mirrored. That is a legitimate configuration, but a batch full of
			// deletions that quietly does nothing looks exactly like a bug, so
			// say it once per process rather than every batch.
			r.dropWarned = true
			r.Logf("info", "%d runtime deletion(s) not mirrored: delete = \"startup\" reconciles only during the initial sync", len(deletes))
		}
	}
	if partial != nil {
		return partial
	}
	return nil
}

// plan splits a batch into the paths rsync should transfer and the paths it
// should remove.
//
// Delete-only changes cannot be expressed through --files-from at all: rsync
// would look for a file that no longer exists and silently skip the removal, so
// they are collected separately.
//
// A rename is one change carrying both names. rsync has no client-side rename,
// so the new name is transferred and the old name has to be removed explicitly;
// otherwise every "mv a b" leaves a duplicate of a on the target forever. The
// direct backend renames in place, and this is what keeps the two equivalent.
func (r *RSync) plan(changes []aggregate.Change) (uploads, deletes []string) {
	for _, c := range changes {
		if c.Op&event.Delete != 0 && c.Op&(event.Create|event.Write) == 0 {
			deletes = append(deletes, c.Path)
			continue
		}
		rel, ok := relInSource(r.Source, c.Path)
		if !ok {
			continue
		}
		uploads = append(uploads, filepath.ToSlash(rel))
		if c.From != "" && c.Op&event.Move != 0 {
			deletes = append(deletes, c.From)
		}
	}
	return uploads, deletes
}

func (r *RSync) upload(ctx context.Context, rels []string) error {
	r.Logf("debug", "%s: sending %d paths", r.Name(), len(rels))
	args := r.baseArgs()
	args = append(args, strings.TrimSuffix(r.Source, string(filepath.Separator))+string(filepath.Separator), r.dest())
	reader := newNulReader(rels)
	return r.run(ctx, args, reader)
}

// deleteArgs builds the remote deletion invocation.
//
// Two traps were measured on a real target and both are load bearing:
//
//   - --delete must NOT appear. Combined with --files-from it makes rsync
//     treat every destination entry absent from the list as extraneous, so
//     deleting one file would wipe the rest of a 100 TB mirror.
//     --delete-missing-args removes exactly the listed paths.
//
//   - --force must appear. Without it rsync prints "cannot delete non-empty
//     directory: keep" and still exits 0, so a renamed directory silently
//     leaves its entire old subtree behind and the mirror drifts. With it the
//     listed directory is removed together with its contents, which is what a
//     local target's RemoveAll does.
func (r *RSync) deleteArgs() []string {
	args := []string{"-a", "--delete-missing-args", "--force", "--from0", "--files-from=-"}
	if rsh := r.rshArg(); rsh != "" {
		args = append(args, rsh)
	}
	args = append(args, withoutDeleteFlags(r.Extra)...)
	return append(args, strings.TrimSuffix(r.Source, string(filepath.Separator))+string(filepath.Separator), r.dest())
}

// deleteList removes paths on the target. For a local target this is a plain
// unlink, which is both faster and more accurate than an rsync run; for a
// remote target rsync --delete-missing-args is used when available.
func (r *RSync) deleteList(ctx context.Context, abs []string) error {
	if r.SSH {
		rels := make([]string, 0, len(abs))
		for _, p := range abs {
			rel, ok := relInSource(r.Source, p)
			if !ok {
				continue
			}
			rels = append(rels, filepath.ToSlash(rel))
		}
		return r.run(ctx, r.deleteArgs(), newNulReader(rels))
	}
	var firstErr error
	for _, p := range abs {
		rel, ok := relInSource(r.Source, p)
		if !ok {
			continue
		}
		dst := filepath.Join(r.Target, rel)
		if err := os.RemoveAll(dst); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// benignExit reports whether an rsync exit status means "the batch was applied,
// but some paths raced with a concurrent change".
//
// rsync's documented codes are:
//
//	23  partial transfer: some paths could not be transferred
//	24  partial transfer: some source files vanished before they were read
//
// During a mass delete (rm -rf of a large subtree) every batch races with the
// tree disappearing underneath it, so 24 is the *normal* outcome, not a
// failure. Treating it as fatal is what makes issue #695 look like a hang: each
// batch "fails", the retry gate backs off, the aggregate window is re-armed as
// an overflow, and a full rescan is scheduled for a transfer that actually
// delivered everything that still existed.
//
// Code 23 is treated the same way but reported: unlike 24 it can also mean a
// genuinely unreadable file, and on a 100 TB tree an operator needs to see that
// once rather than never.
func (r *RSync) benignExit(err error) bool {
	switch ExitCode(err) {
	case 23:
		r.warnPartial()
		return true
	case 24:
		return true
	}
	return false
}

// warnPartial reports an exit-23 at most once a minute.
func (r *RSync) warnPartial() {
	r.partialMu.Lock()
	now := time.Now()
	if !r.partialAt.IsZero() && now.Sub(r.partialAt) < time.Minute {
		r.partialMu.Unlock()
		return
	}
	r.partialAt = now
	r.partialMu.Unlock()
	r.Logf("warn", "%s: partial transfer, some paths could not be read; they stay queued for the next window", r.Name())
}

func (r *RSync) run(ctx context.Context, args []string, stdin *nulReader) error {
	rc := RunConfig{
		Path:      r.binary(),
		Args:      args,
		Timeout:   r.Timeout,
		MaxStderr: 16384,
	}
	if rc.Timeout == 0 {
		// A stuck rsync on a dead link must not block the queue forever; the
		// delay is generous because a single huge file can legitimately take
		// a long time.
		rc.Timeout = 6 * time.Hour
	}
	if stdin != nil {
		rc.Stdin = stdin
	}
	if r.OnLine != nil {
		rc.OnLine = r.OnLine
	}
	r.Logf("debug", "%s %s", rc.Path, strings.Join(args, " "))
	return Run(ctx, rc)
}

// nulReader streams a NUL separated path list, which is what --from0 expects.
type nulReader struct {
	items []string
	buf   []byte
	pos   int
}

func newNulReader(items []string) *nulReader { return &nulReader{items: items} }

func (n *nulReader) Read(p []byte) (int, error) {
	for len(n.buf) == 0 {
		if n.pos >= len(n.items) {
			return 0, io.EOF
		}
		n.buf = append(n.buf, n.items[n.pos]...)
		n.buf = append(n.buf, 0)
		n.pos++
	}
	c := copy(p, n.buf)
	n.buf = n.buf[c:]
	return c, nil
}
