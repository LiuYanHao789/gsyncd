package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
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
	// Remote says the destination is reached through rsync's remote syntax
	// (host:path, host::module, rsync://) rather than being a local path.
	// It is separate from SSH because mode = "rsync" can also name a remote
	// target, and local filesystem operations such as RemoveAll are then
	// meaningless. Keeping the two apart is what makes runtime deletion work
	// for both dialects instead of silently acting on a local path named
	// "host:path".
	Remote bool
	// Extra arguments appended verbatim before the source/target pair.
	Extra []string
	// Binary overrides the rsync executable (GSYNCD_RSYNC still wins for
	// tests, since that is how a fake rsync is injected).
	Binary string
	// BWLimit is the transfer cap in bytes per second; rsync's own unit is
	// KiB per second.
	BWLimit int64
	Compress     bool
	Xattrs       bool
	ACLs         bool
	HardLinks    bool
	Chown        string
	Chmod        string
	PasswordFile string
	RsyncPath    string
	// DeleteExcluded removes excluded destination entries on a whole-tree
	// transfer. Ignored on incremental runs.
	DeleteExcluded bool
	// SymlinkMode mirrors the direct backend: safe adds --safe-links, all
	// leaves rsync's default -l, skip adds --no-links.
	SymlinkMode string
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
	// is on a real mount or carries the marker. It is skipped for remote
	// targets, where Target names a host and path on another machine.
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

// remote reports whether the destination has to be reached through rsync's
// remote protocol. Both dialects count: rsyncssh always does, and mode = "rsync"
// does when the target carries a host part.
func (r *RSync) remote() bool { return r.SSH || r.Remote }

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
	// The environment variable stays first so a test or a rescue shell can
	// replace the binary without editing the configuration.
	if b := os.Getenv("GSYNCD_RSYNC"); b != "" {
		return b
	}
	if r.Binary != "" {
		return r.Binary
	}
	return "rsync"
}

// commonArgs builds the flags shared by every whole-tree or incremental
// transfer. It does not include --files-from or the source/target operands.
func (r *RSync) commonArgs() []string {
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
	args := []string{"-a", "-r"}
	switch r.SymlinkMode {
	case "skip":
		args = append(args, "--no-links")
	case "all":
		// rsync's default -l copies the link text verbatim, including links
		// pointing outside the tree; that is the faithful "all" mode.
	default:
		// --safe-links makes rsync ignore symlinks whose target escapes the
		// copied tree, matching the direct backend's "internal links only"
		// default.
		args = append(args, "--safe-links")
	}
	if r.DeleteExcluded {
		// --delete-excluded overrides --exclude and would remove the target
		// guard marker, which exists only on the target. A protect rule keeps
		// it while still excluding it from the transfer.
		args = append(args, "--filter=P "+TargetMarkerName)
	}
	args = append(args, "--exclude="+TargetMarkerName)
	if r.Compress {
		args = append(args, "--compress")
	}
	if r.Xattrs {
		args = append(args, "--xattrs")
	}
	if r.ACLs {
		args = append(args, "--acls")
	}
	if r.HardLinks {
		args = append(args, "--hard-links")
	}
	if r.BWLimit > 0 {
		// rsync's unit is KiB per second; a sub-KiB request must not become
		// 0, which rsync reads as "unlimited".
		kib := r.BWLimit / 1024
		if kib < 1 {
			kib = 1
		}
		args = append(args, fmt.Sprintf("--bwlimit=%d", kib))
	}
	if r.Chown != "" {
		args = append(args, "--chown="+r.Chown)
	}
	if r.Chmod != "" {
		args = append(args, "--chmod="+r.Chmod)
	}
	if r.PasswordFile != "" {
		args = append(args, "--password-file="+r.PasswordFile)
	}
	if r.RsyncPath != "" {
		args = append(args, "--rsync-path="+r.RsyncPath)
	}
	// Keep the transfer restartable after an interruption: a 100 TB tree must
	// not begin again from zero because one batch failed.
	args = append(args, "--partial", "--partial-dir=.rsync-partial")
	return args
}

// baseArgs builds the arguments shared by every incremental invocation.
func (r *RSync) baseArgs() []string {
	args := r.commonArgs()
	args = append(args, "--from0", "--files-from=-")
	// --delete is deliberately absent here: an incremental run carries only the
	// changed paths, and rsync would then consider everything else extraneous
	// (see deleteList). Only Startup, which mirrors the whole tree, adds it.
	if r.SSH {
		args = append(args, r.rshArg())
	}
	// A user-supplied --delete in rsync_extra would turn an incremental
	// --files-from run into "delete everything not in this batch". It is
	// stripped here and added only by the whole-tree paths, which know what
	// they are mirroring.
	args = append(args, withoutDeleteFlags(r.Extra)...)
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
	if r.TargetGuard && !r.remote() {
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
		if r.DeleteExcluded {
			filtered = append(filtered, "--delete-excluded")
		}
	}
	filtered = append(filtered, src, r.dest())
	if err := r.run(ctx, filtered, nil); err != nil {
		return err
	}
	if r.Delete && r.DeleteOnStartup && !r.remote() {
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

// uploadSubtree mirrors one whole directory that a filter split turned into a
// Create. Unlike an ordinary batch it does not use --files-from: the point is
// to walk the directory recursively, and --delete (when deletion is enabled)
// makes the destination match the moved subtree instead of keeping entries the
// source no longer has.
func (r *RSync) uploadSubtree(ctx context.Context, c aggregate.Change) error {
	rel, ok := relInSource(r.Source, c.Path)
	if !ok {
		return nil
	}
	// Do not destroy the destination for a source that is already gone. The
	// move event named it, but a race with a delete is possible; removing
	// first and then failing to upload would leave the mirror emptier than
	// either side of the race.
	if _, err := os.Lstat(c.Path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// The path was renamed again after this window was taken. The
			// follow-up move carries the new name; failing here would re-add
			// this stale Subtree change forever and block the whole root.
			return nil
		}
		return err
	}
	// A rename replaces its destination. Remove it first, exactly as the
	// direct backend does, instead of asking rsync for a scoped --delete.
	if err := r.removeDest(ctx, c.Path); err != nil {
		return err
	}
	// Transfer through the normal sync-root-relative path list. Running rsync
	// with the moved subtree as its transfer root would re-base anchored
	// exclude patterns (/ok/moved/secret/) and silently copy content that the
	// same configuration excludes in direct mode.
	return r.upload(ctx, []string{filepath.ToSlash(rel)})
}

// removeDest deletes one destination path, local or remote.
func (r *RSync) removeDest(ctx context.Context, abs string) error {
	rel, ok := relInSource(r.Source, abs)
	if !ok {
		return nil
	}
	if r.remote() {
		return r.remoteDelete(ctx, []string{filepath.ToSlash(rel)})
	}
	if err := os.RemoveAll(filepath.Join(r.Target, rel)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// shellQuote wraps s in single quotes so a remote shell treats it as one word.
// Single quotes inside are closed, escaped and reopened, which is the only
// portable way to pass an arbitrary path.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// sshPath returns the absolute remote path for a source-relative path.
func (r *RSync) sshPath(rel string) string {
	dir := r.TargetDir
	if dir == "" {
		dir = r.Source
	}
	return path.Join(dir, rel)
}

// sshRun executes a shell script on the remote host through ssh's stdin.
// Sending the script on stdin (sh -s) avoids the ARG_MAX ceiling that a
// command-line list of paths would hit on a large rename or delete batch.
func (r *RSync) sshRun(ctx context.Context, script string) error {
	bin := r.SSHOpts.Binary
	if bin == "" {
		bin = "ssh"
	}
	args := make([]string, 0, 8+len(r.SSHOpts.Extra))
	if r.SSHOpts.Port > 0 {
		args = append(args, "-p", strconv.Itoa(r.SSHOpts.Port))
	}
	if r.SSHOpts.IdentityFile != "" {
		args = append(args, "-i", r.SSHOpts.IdentityFile)
	}
	args = append(args, "-o", "BatchMode=yes")
	args = append(args, r.SSHOpts.Extra...)
	args = append(args, r.Target, "sh -s")
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = time.Hour
	}
	return Run(ctx, RunConfig{
		Path:      bin,
		Args:      args,
		Stdin:     strings.NewReader(script),
		Timeout:   timeout,
		MaxStderr: 8192,
	})
}

// sshMoveChanges renames every path on the remote host in one ssh invocation.
// It is the rsyncssh optimisation the original lsyncd is known for: a
// directory rename costs a remote mv, not a retransfer of the whole tree.
// Ancestors are moved first, so a child rename inside a renamed directory
// still finds its source.
func (r *RSync) sshMoveChanges(ctx context.Context, changes []aggregate.Change) bool {
	type moveOp struct {
		src string
		dst string
	}
	ops := make([]moveOp, 0, len(changes))
	for _, c := range changes {
		if c.From == "" || c.Op&event.Delete != 0 {
			continue
		}
		if c.Op&(event.Create|event.Write|event.Attrib) != 0 {
			// The rename carries content or metadata that only rsync can
			// apply; moving the old remote file would keep the stale copy.
			continue
		}
		fromRel, ok1 := relInSource(r.Source, c.From)
		toRel, ok2 := relInSource(r.Source, c.Path)
		if !ok1 || !ok2 {
			// A path outside the root is a routing error, not a move this
			// invocation can take responsibility for.
			return false
		}
		ops = append(ops, moveOp{
			src: r.sshPath(filepath.ToSlash(fromRel)),
			dst: r.sshPath(filepath.ToSlash(toRel)),
		})
	}
	if len(ops) == 0 {
		return false
	}
	// A batch of rm+mv operations is only safe when none of the paths can
	// destroy another operation's source or destination. Two moves into
	// nested destinations, a chain a->b, b->c, or a destination under another
	// move's source would otherwise silently lose data.
	srcSet := make(map[string]bool, len(ops))
	dstSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		srcSet[op.src] = true
		dstSet[op.dst] = true
	}
	for _, op := range ops {
		if srcSet[op.dst] || hasStrictAncestor(srcSet, op.dst) ||
			dstSet[op.src] || hasStrictAncestor(dstSet, op.src) ||
			hasStrictAncestor(dstSet, op.dst) {
			r.Logf("debug", "%s: overlapping renames, falling back to rsync", r.Name())
			return false
		}
	}
	sort.SliceStable(ops, func(i, j int) bool {
		di := strings.Count(ops[i].src, "/")
		dj := strings.Count(ops[j].src, "/")
		if di != dj {
			return di < dj
		}
		return ops[i].src < ops[j].src
	})
	var b strings.Builder
	b.WriteString("set -e\n")
	for _, op := range ops {
		// `mv a b` nests a inside b when b is an existing directory; a
		// rename replaces its destination. Remove the destination first.
		b.WriteString("rm -rf -- ")
		b.WriteString(shellQuote(op.dst))
		b.WriteByte('\n')
		b.WriteString("mkdir -p -- ")
		b.WriteString(shellQuote(path.Dir(op.dst)))
		b.WriteString("\n")
		b.WriteString("mv -f -- ")
		b.WriteString(shellQuote(op.src))
		b.WriteByte(' ')
		b.WriteString(shellQuote(op.dst))
		b.WriteByte('\n')
	}
	if err := r.sshRun(ctx, b.String()); err != nil {
		r.Logf("debug", "%s: remote move failed, falling back to rsync: %v", r.Name(), err)
		return false
	}
	return true
}

// hasStrictAncestor reports whether any proper ancestor of p is in set.
func hasStrictAncestor(set map[string]bool, p string) bool {
	for {
		if p == "/" {
			return false
		}
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			return false
		}
		if i == 0 {
			return set["/"]
		}
		p = p[:i]
		if set[p] {
			return true
		}
	}
}

// sshDeleteChanges removes paths on the remote host with rm -rf in one ssh
// invocation. A directory rename whose old subtree still exists is the common
// case this makes free; falling back to rsync --delete-missing-args is correct
// but much slower.
func (r *RSync) sshDeleteChanges(ctx context.Context, changes []aggregate.Change) bool {
	var b strings.Builder
	b.WriteString("set -e\n")
	n := 0
	for _, c := range changes {
		if c.Op&event.Delete == 0 || c.Op&(event.Create|event.Write) != 0 {
			continue
		}
		paths := []string{c.Path}
		if c.From != "" && c.Op&event.Move != 0 {
			// mv a b && rm b: both names are gone at the source.
			paths = append(paths, c.From)
		}
		paths = append(paths, c.PrevFrom...)
		for _, p := range paths {
			rel, ok := relInSource(r.Source, p)
			if !ok {
				// Same reasoning as sshMoveChanges: a path outside the root
				// must not be reported as handled by a skipped invocation.
				return false
			}
			b.WriteString("rm -rf -- ")
			b.WriteString(shellQuote(r.sshPath(filepath.ToSlash(rel))))
			b.WriteByte('\n')
			n++
		}
	}
	if n == 0 {
		return false
	}
	if err := r.sshRun(ctx, b.String()); err != nil {
		r.Logf("debug", "%s: remote delete failed, falling back to rsync: %v", r.Name(), err)
		return false
	}
	return true
}

// Apply implements Backend by streaming the changed relative paths to rsync.
func (r *RSync) Apply(ctx context.Context, b Batch) error {
	if b.Full {
		return r.Startup(ctx)
	}
	if len(b.Changes) == 0 {
		return nil
	}
	if r.TargetGuard && !r.remote() && r.targetInfo != nil {
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
		var remoteRecreates []string
		for _, c := range b.Changes {
			if !c.Recreate {
				continue
			}
			rel, ok := relInSource(r.Source, c.Path)
			if !ok {
				continue
			}
			if r.remote() {
				remoteRecreates = append(remoteRecreates, c.Path)
				continue
			}
			if err := os.RemoveAll(filepath.Join(r.Target, rel)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		// One rsync invocation for all recreated directories, not one temp
		// dir and child process per directory.
		if err := r.deleteList(ctx, remoteRecreates); err != nil {
			return err
		}
	}

	// rsyncssh can do moves and deletes on the target host itself. That is
	// the whole point of the dialect: a directory rename becomes a remote mv
	// instead of a retransfer. A change that also carries content (a write
	// folded into a rename) is left to rsync, because mv would move the stale
	// remote copy.
	work := b.Changes
	if r.SSH {
		var moves, dels []aggregate.Change
		var moveIdx, delIdx []int
		for i, c := range work {
			switch {
			case c.From != "" && len(c.PrevFrom) == 0 &&
				c.Op&event.Delete == 0 && c.Op&(event.Create|event.Write|event.Attrib) == 0:
				// A rename is not a deletion: it has to be applied even when
				// delete = "startup" forbids runtime removals, otherwise the
				// old name would stay on the target as a duplicate. A change
				// with several old names is left to plan(), which can delete
				// all of them.
				moves = append(moves, c)
				moveIdx = append(moveIdx, i)
			case r.DeleteOnRun && c.Op&event.Delete != 0 && c.Op&(event.Create|event.Write) == 0:
				dels = append(dels, c)
				delIdx = append(delIdx, i)
			}
		}
		handled := make(map[int]bool, len(moveIdx)+len(delIdx))
		if r.sshMoveChanges(ctx, moves) {
			for _, i := range moveIdx {
				handled[i] = true
			}
			atomic.AddUint64(&r.st.Files, uint64(len(moves)))
		}
		if r.sshDeleteChanges(ctx, dels) {
			for _, i := range delIdx {
				handled[i] = true
			}
			atomic.AddUint64(&r.st.Deletes, uint64(len(dels)))
		}
		if len(handled) > 0 {
			filtered := make([]aggregate.Change, 0, len(work)-len(handled))
			for i, c := range work {
				if !handled[i] {
					filtered = append(filtered, c)
				}
			}
			work = filtered
		}
	}

	// A directory moved out of an excluded path is represented as a subtree
	// Create. It needs its own recursive invocation because --files-from
	// semantics only list paths; the subtree contents were never events.
	for _, c := range work {
		if !c.Subtree || c.Kind != aggregate.KindDir || c.Op&event.Create == 0 {
			continue
		}
		if err := r.uploadSubtree(ctx, c); err != nil {
			atomic.AddUint64(&r.st.Errors, 1)
			return err
		}
		atomic.AddUint64(&r.st.Files, 1)
	}

	uploads, deletes, moveDeletes := r.plan(work)
	// A rename chain (mv a b; mv c a) leaves a as both an upload destination
	// and the old name of another rename. Deleting it would remove the file
	// that was just uploaded; the final source state says it exists.
	if len(moveDeletes) > 0 {
		uploaded := make(map[string]bool, len(uploads))
		for _, u := range uploads {
			uploaded[u] = true
		}
		kept := make([]string, 0, len(moveDeletes))
		for _, md := range moveDeletes {
			if rel, ok := relInSource(r.Source, md); ok && uploaded[filepath.ToSlash(rel)] {
				continue
			}
			// The old name may have been recreated at the source after the
			// move (a rename chain, or a new file at the same path). Deleting
			// the target copy would remove content the source still has.
			if _, err := os.Lstat(md); err == nil {
				continue
			}
			kept = append(kept, md)
		}
		moveDeletes = kept
	}
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
	deleteSet := func(list []string) error {
		if len(list) == 0 {
			return nil
		}
		err := r.deleteList(ctx, list)
		if err == nil {
			atomic.AddUint64(&r.st.Deletes, uint64(len(list)))
			return nil
		}
		if r.benignExit(err) {
			if ExitCode(err) == 23 && partial == nil {
				partial = err
			}
			atomic.AddUint64(&r.st.Deletes, uint64(len(list)))
			return nil
		}
		atomic.AddUint64(&r.st.Errors, 1)
		return err
	}
	if partial != nil {
		// The new name of a rename may not have landed (exit 23/24). Removing
		// the old name now could leave neither copy; retry the batch first.
		return partial
	}
	// The old name of a rename is not a runtime deletion: it is removed even
	// when delete = "startup" suppresses ordinary deletes.
	if err := deleteSet(moveDeletes); err != nil {
		return err
	}
	if len(deletes) > 0 {
		if r.DeleteOnRun {
			if err := deleteSet(deletes); err != nil {
				return err
			}
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
func (r *RSync) plan(changes []aggregate.Change) (uploads, deletes, moveDeletes []string) {
	for _, c := range changes {
		// Subtree Creates were already mirrored by uploadSubtree; listing them
		// here as well would send the same directory twice.
		if c.Subtree {
			continue
		}
		if c.Op&event.Delete != 0 && c.Op&event.Create == 0 {
			// A path written and then deleted inside one window ends deleted;
			// the Write bit must not turn it into an upload.
			deletes = append(deletes, c.Path)
			if c.From != "" && c.Op&event.Move != 0 {
				// mv a b && rm b leaves neither name at the source.
				moveDeletes = append(moveDeletes, c.From)
			}
			moveDeletes = append(moveDeletes, c.PrevFrom...)
			continue
		}
		rel, ok := relInSource(r.Source, c.Path)
		if !ok {
			continue
		}
		uploads = append(uploads, filepath.ToSlash(rel))
		if c.From != "" && c.Op&event.Move != 0 {
			// The old name of a rename is not a runtime deletion. It has to
			// disappear even under delete = "startup", or a failed ssh fast
			// path would leave a duplicate that no later event removes.
			moveDeletes = append(moveDeletes, c.From)
		}
		moveDeletes = append(moveDeletes, c.PrevFrom...)
	}
	return uploads, deletes, moveDeletes
}

func (r *RSync) upload(ctx context.Context, rels []string) error {
	r.Logf("debug", "%s: sending %d paths", r.Name(), len(rels))
	args := r.baseArgs()
	args = append(args, strings.TrimSuffix(r.Source, string(filepath.Separator))+string(filepath.Separator), r.dest())
	reader := newNulReader(rels)
	return r.run(ctx, args, reader)
}

// remoteDelete removes the listed relative paths on a remote target.
//
// It runs rsync against an empty temporary source and lists the paths with
// --delete-missing-args. Two traps are load bearing:
//
//   - --delete must NOT appear. Combined with --files-from it makes rsync
//     treat every destination entry absent from the list as extraneous, so
//     deleting one file would wipe the rest of a 100 TB mirror.
//     --delete-missing-args removes exactly the listed paths.
//
//   - The source must be empty. Pointing at the sync source would make every
//     listed path that still exists there a transfer, not a deletion; that is
//     the Recreate and Subtree case, where the old destination has to go
//     before the new content arrives.
//
//   - --force must appear. Without it rsync prints "cannot delete non-empty
//     directory: keep" and still exits 0, so a renamed directory silently
//     leaves its entire old subtree behind and the mirror drifts. With it the
//     listed directory is removed together with its contents, which is what a
//     local target's RemoveAll does.
func (r *RSync) remoteDelete(ctx context.Context, rels []string) error {
	if len(rels) == 0 {
		return nil
	}
	empty, err := os.MkdirTemp("", "gsyncd-del-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(empty)
	args := r.deleteArgs()
	args = append(args, empty+string(filepath.Separator), r.dest())
	return r.run(ctx, args, newNulReader(rels))
}

// deleteArgs builds the flags shared by the remote deletion invocation.
func (r *RSync) deleteArgs() []string {
	// -r is explicit: with --files-from, -a does not imply -r, and a listed
	// directory that is not recursed is never removed with its contents.
	args := []string{"-a", "-r", "--delete-missing-args", "--force", "--from0", "--files-from=-"}
	if r.SSH {
		args = append(args, r.rshArg())
	}
	if r.PasswordFile != "" {
		args = append(args, "--password-file="+r.PasswordFile)
	}
	if r.RsyncPath != "" {
		args = append(args, "--rsync-path="+r.RsyncPath)
	}
	return append(args, withoutDeleteFlags(r.Extra)...)
}

// deleteList removes paths on the target. For a local target this is a plain
// unlink, which is both faster and more accurate than an rsync run; for a
// remote target rsync --delete-missing-args is used.
func (r *RSync) deleteList(ctx context.Context, abs []string) error {
	if r.remote() {
		rels := make([]string, 0, len(abs))
		for _, p := range abs {
			rel, ok := relInSource(r.Source, p)
			if !ok {
				continue
			}
			rels = append(rels, filepath.ToSlash(rel))
		}
		return r.remoteDelete(ctx, rels)
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
