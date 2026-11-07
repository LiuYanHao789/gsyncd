// Package engine wires the watcher, the aggregator and the transfer backends
// into a running daemon.
//
// The original lsyncd ran everything on one Lua coroutine inside a select()
// loop: event intake, delay bookkeeping and the sync decision all shared a
// single thread, and each decision forked a new rsync. Here intake, collapse
// and transfer are separate goroutines, a per-sync delay timer decides when a
// window closes, and the number of concurrent transfers is bounded by a
// semaphore instead of being whatever the fork storm produced.
package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
	"gsyncd/internal/filter"
	"gsyncd/internal/logx"
	syncpkg "gsyncd/internal/sync"
	"gsyncd/internal/watcher"
)

// Spec describes one synchronisation.
type Spec struct {
	Name    string
	Source  string
	Delay   time.Duration
	Filter  *filter.Filter
	Backend syncpkg.Backend
	// MaxPending caps the aggregation window; 0 means the global default.
	MaxPending int
	// MaxHold bounds how long a pending change may wait for the tree to fall
	// silent, which is what a delay normally means - but a file that is
	// written to continuously (a log, a WAL, a database growing in place)
	// never produces silence, and with no bound its changes would sit in the
	// window until the writer exits (issues #77, #458). With it, such a path
	// is copied once per MaxHold, so the mirror trails a busy writer by at
	// most that much. Zero selects DefaultMaxHold; a negative value disables
	// the bound and restores "wait for silence forever".
	MaxHold time.Duration
	// CloseWrite holds a flush back until every pending file has been closed
	// by its last writer, instead of copying as soon as the tree is quiet for
	// Delay (issue #458).
	//
	// It is opt-in because the two failure modes are not comparable. Without
	// it, a large file that is being written is copied mid-write and the
	// mirror holds a torn snapshot until the writer closes - visible, and
	// self-healing at close. With it, a file that is opened once and never
	// closed (a database datafile, an mmap'd index, a file held by a daemon)
	// is never copied at all, and the mirror stays silently at the version
	// from before the daemon started - invisible, and not self-healing. So
	// the default stays "copy on quiet", and enabling this is a statement
	// that the tree contains files which do get closed.
	//
	// MaxHold must stay enabled with it: it is the only progress guarantee
	// left for a writer that never closes.
	CloseWrite bool
	// Retries is how often a failing batch is attempted before it is put back
	// into the window for the next round.
	Retries int
	// MaxProcesses is the per-sync ceiling on concurrent transfers. Zero means
	// "only bounded by the global one". It exists because the global cap alone
	// lets one busy sync occupy every slot while a small, urgent sync waits.
	MaxProcesses int
}

// Options configures the engine.
type Options struct {
	// MaxProcesses bounds concurrent transfers across all syncs.
	MaxProcesses int
	// Tick is the scheduler resolution; the default is 200ms.
	Tick time.Duration
	Log  *logx.Logger
	Now  func() time.Time
}

// Stats counts what one sync has done.
type Stats struct {
	Batches  uint64
	Events   uint64
	Filtered uint64
	Applied  uint64
	// Files and Deletes come from the backend, which is the only layer that
	// knows what actually reached the target: the engine counts batches and
	// collapsed changes, not files written or entries removed.
	Files     uint64
	Deletes   uint64
	Errors    uint64
	Retries   uint64
	FullSyncs uint64
	Overflows uint64
	// RootGone counts how often the watch on this sync's root had to be
	// rebuilt. Non-zero means the tree was invisible for a while, so the
	// numbers around it describe a period of blindness.
	RootGone uint64
	// Starting is true while this sync's initial transfer is still running.
	// On a large tree that is hours, and it is the one state where every
	// other counter reads zero while the daemon is working flat out - so an
	// operator must be able to tell it apart from an idle or stuck process.
	Starting  bool
	LastError string
	LastFlush time.Time
	Pending   int
	Peak      int
	// Open counts pending changes whose file was written but not yet closed.
	// It is zero unless close_write is on, and non-zero is the only reason a
	// window with pending work is deliberately not being flushed - without
	// this an operator sees "pending: 40" that never moves and no explanation.
	Open int
}

// Root is the runtime state of one sync.
type Root struct {
	Spec
	agg *aggregate.Aggregator

	mu        sync.Mutex
	lastEvent time.Time
	flushing  bool
	pending   bool
	// retryAt gates the next attempt after a failed round, and fails counts
	// the consecutive failures that set it. Without the gate a target that is
	// down would be retried as fast as the delay allows - and a failed *full*
	// resync would rescan the entire tree every few seconds, which on a large
	// mirror is worse for the source disks than being idle.
	retryAt time.Time
	fails   int
	// lost is set when the watcher reports that the watch on this sync's root
	// disappeared. The tree is invisible until it is re-attached, so the
	// supervisor has to rebuild the watch and then re-transfer everything:
	// whatever changed while the root was gone left no events at all.
	lost bool
	st   Stats
	// limit is the per-sync transfer semaphore, nil when the sync inherits the
	// global one.
	limit chan struct{}
}

// statsSource is implemented by backends that can report what they wrote.
// It is optional so a test double or a future backend does not have to.
type statsSource interface {
	Stats() syncpkg.Stats
}

// Stats returns a copy of the counters.
func (r *Root) Stats() Stats {
	r.mu.Lock()
	s := r.st
	r.mu.Unlock()
	s.Pending = r.agg.Len()
	s.Open = r.agg.OpenFiles()
	// The engine only sees batches; how many files were written and how many
	// entries were removed is known to the backend alone. Merging here keeps
	// the status file honest: without it "deletes" would read zero forever,
	// and an operator watching a batch delete drain would see a frozen field
	// and conclude the run had stalled.
	if bs, ok := r.Backend.(statsSource); ok {
		b := bs.Stats()
		s.Files = b.Files
		s.Deletes = b.Deletes
	}
	return s
}

// Pending reports the current window size.
func (r *Root) Pending() int { return r.agg.Len() }

// Engine is the running daemon core.
type Engine struct {
	opts  Options
	log   *logx.Logger
	roots []*Root
	sem   chan struct{}
	wg    sync.WaitGroup
	now   func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// fullEvery is the interval of the periodic wholesale resync, in
	// nanoseconds. Zero disables it. Stored atomically because the caller may
	// set it after the engine started running.
	fullEvery int64
	nextFull  time.Time
}

// SetFullResync schedules a periodic wholesale resynchronisation of every
// root. It is the safety net for trees the kernel cannot watch completely:
// when /proc/sys/fs/inotify/max_user_watches is exhausted, or on a platform
// with no recursive notification API, events under the unwatched subtrees are
// simply invisible, so the only way to stay consistent is to re-verify the
// tree from time to time. Zero disables it.
func (e *Engine) SetFullResync(d time.Duration) {
	if d < 0 {
		d = 0
	}
	atomic.StoreInt64(&e.fullEvery, int64(d))
}

// FullResync returns the configured interval, for the status file.
func (e *Engine) FullResync() time.Duration {
	return time.Duration(atomic.LoadInt64(&e.fullEvery))
}

// DefaultMaxHold is how long a pending change may wait for a quiet tree before
// it is copied anyway.
//
// It has to be long enough that ordinary bursts of writes to one file - a
// multi-gigabyte copy in progress, a database checkpoint - do not turn into
// repeated full copies of that file, and short enough that a mirror of a busy
// append-only log converges within a shift. An hour satisfies both: a file
// written continuously costs at most one copy per hour, and one that merely
// takes twenty minutes to upload is copied once, when it stops.
const DefaultMaxHold = time.Hour

// New builds an engine.
func New(specs []Spec, opts Options) (*Engine, error) {
	if opts.MaxProcesses <= 0 {
		opts.MaxProcesses = 8
	}
	if opts.Tick <= 0 {
		opts.Tick = 200 * time.Millisecond
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Log == nil {
		opts.Log = logx.NewStderr(logx.Info)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{opts: opts, log: opts.Log, sem: make(chan struct{}, opts.MaxProcesses), now: opts.Now, ctx: ctx, cancel: cancel}
	for _, s := range specs {
		if s.Name == "" {
			s.Name = filepath.Base(s.Source)
		}
		if s.Delay < 0 {
			s.Delay = 0
		}
		if s.Retries <= 0 {
			s.Retries = 3
		}
		if s.CloseWrite && s.MaxHold < 0 {
			// The combination means "wait for a close that may never come,
			// with no upper bound". That is not a preference, it is a mirror
			// that silently stops updating, so refuse it instead of shipping
			// a configuration whose failure is invisible.
			return nil, fmt.Errorf("sync %q: close_write needs max_hold; without it a file that is never closed is never copied", s.Name)
		}
		if s.MaxHold == 0 {
			s.MaxHold = DefaultMaxHold
		}
		if s.MaxHold < 0 {
			// Explicitly disabled: only silence closes a window.
			s.MaxHold = 0
		}
		r := &Root{Spec: s}
		if s.MaxProcesses > 0 {
			r.limit = make(chan struct{}, s.MaxProcesses)
		}
		r.agg = aggregate.New(aggregate.Options{MaxPending: s.MaxPending, Now: opts.Now})
		e.roots = append(e.roots, r)
	}
	// Longest source first so lookup() finds the most specific root by
	// taking the first prefix match.
	sort.SliceStable(e.roots, func(i, j int) bool {
		return len(e.roots[i].Source) > len(e.roots[j].Source)
	})
	return e, nil
}

// Reattach reports that a sync root came back after losing its watch. The tree
// was invisible for a while, so everything that changed during that window left
// no events at all: the only way to be consistent again is one full transfer.
// It also clears the retry gate, because a failure caused by the missing root
// says nothing about the target being reachable.
func (e *Engine) Reattach(source string) {
	r := e.lookup(filepath.Clean(source))
	if r == nil {
		return
	}
	r.mu.Lock()
	r.lost = false
	r.fails = 0
	r.retryAt = time.Time{}
	r.mu.Unlock()
	r.agg.MarkOverflow()
	// Flush now instead of at the next tick: the tree is unwatched until this
	// flush finishes, so waiting for the delay would extend the blind window
	// for no reason.
	e.flushAsync(r)
}

// LostRoots lists the syncs whose root is currently un-watched.
func (e *Engine) LostRoots() []string {
	var out []string
	for _, r := range e.roots {
		r.mu.Lock()
		lost := r.lost
		r.mu.Unlock()
		if lost {
			out = append(out, r.Source)
		}
	}
	return out
}

// Roots exposes the runtime state for the status file.
func (e *Engine) Roots() []*Root { return e.roots }

// Startup runs the initial synchronisation of every root in parallel under the
// process cap. The original ran them serially while all of them still hit the
// disk at once, which is what filled the disks in issue #75.
func (e *Engine) Startup(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make([]error, len(e.roots))
	for i, r := range e.roots {
		wg.Add(1)
		go func(i int, r *Root) {
			defer wg.Done()
			if !e.acquire(ctx, r) {
				errs[i] = ctx.Err()
				return
			}
			defer e.release(r)
			t0 := e.now()
			// Publish "initial sync running" for the whole duration. On a
			// multi-terabyte tree this is hours during which every counter
			// below reads zero; without this flag the status file of a daemon
			// working at full speed is indistinguishable from a hung one.
			r.mu.Lock()
			r.st.Starting = true
			r.mu.Unlock()
			defer func() {
				r.mu.Lock()
				r.st.Starting = false
				r.mu.Unlock()
			}()
			e.log.Infof("[%s] initial sync %s", r.Name, r.Source)
			if err := r.Backend.Startup(ctx); err != nil {
				errs[i] = err
				e.log.Errorf("[%s] initial sync failed after %s: %v", r.Name, e.now().Sub(t0).Round(time.Millisecond), err)
				// A failed initial sync leaves a half mirrored tree and no
				// event will ever repair it: the files that failed are already
				// on disk at the source, so nothing changes and the watcher
				// stays silent. Arm a full resync behind the retry gate so the
				// tree converges once the cause is gone (a full disk, an
				// unmounted target, a permission fixed by hand) instead of
				// staying silently incomplete until the next restart.
				r.mu.Lock()
				r.fails++
				r.st.Errors++
				r.st.LastError = err.Error()
				r.retryAt = e.now().Add(syncpkg.Backoff(r.fails-1, 15*time.Second, 10*time.Minute))
				r.mu.Unlock()
				r.agg.MarkOverflow()
				return
			}
			e.log.Infof("[%s] initial sync done in %s", r.Name, e.now().Sub(t0).Round(time.Millisecond))
		}(i, r)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Run consumes events until the context is cancelled or the channel closes.
func (e *Engine) Run(ctx context.Context, events <-chan event.Event) error {
	tick := time.NewTicker(e.opts.Tick)
	defer tick.Stop()
	var err error
loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		case ev, ok := <-events:
			if !ok {
				break loop
			}
			e.Handle(ev)
		case now := <-tick.C:
			e.Tick(now)
		}
	}
	// Drain what is already known before returning, so a clean shutdown does
	// not lose the last window. Both calls deliberately ignore ctx: it is
	// already cancelled here (that is why the loop ended), so waiting on it
	// would return instantly and the process would exit while the workers are
	// still writing .gsyncd-tmp-* files, leaving half copied trees behind.
	//
	// The order matters. A transfer started before the cancel may still be
	// running, and flushing synchronously next to it would put two writers on
	// the same target: one copying a file the other is deleting. Waiting for
	// the in-flight batch first, then flushing whatever is left, keeps a single
	// writer per root at all times.
	e.wait(context.Background(), 30*time.Second)
	e.FlushAll(context.Background())
	e.wait(context.Background(), 30*time.Second)
	return err
}

// Handle places one watcher event into the right aggregation window.
func (e *Engine) Handle(ev event.Event) {
	if ev.Op&event.RootGone != 0 {
		// One named tree went dark, not the whole watcher. Mark it so the
		// supervisor re-attaches it, and force a full resync for that root
		// only: the window it has accumulated may describe a tree that has
		// been replaced under it.
		if r := e.lookup(ev.Path); r != nil {
			r.mu.Lock()
			already := r.lost
			r.lost = true
			r.mu.Unlock()
			if !already {
				e.bump(r, func(s *Stats) { s.RootGone++ })
				e.log.Throttlef("rootgone:"+r.Name, time.Minute, logx.Warn,
					"[%s] lost the watch on %s, the tree is invisible until it is re-attached",
					r.Name, ev.Path)
			}
			r.agg.MarkOverflow()
		}
		return
	}
	if ev.Op&event.Overflow != 0 {
		for _, r := range e.roots {
			// Count episodes, not events: the kernel emits one IN_Q_OVERFLOW
			// per dropped batch, so a single burst would otherwise read as
			// nineteen incidents in the status file. r.agg.Overflowed() is
			// still true from the first event until the next flush, which is
			// exactly one episode.
			if !r.agg.Overflowed() {
				e.bump(r, func(s *Stats) { s.Overflows++ })
			}
			r.agg.MarkOverflow()
			// The kernel emits one IN_Q_OVERFLOW per dropped batch, so a
			// burst that gives up three seconds of queue space produces
			// dozens of identical warnings. One line a minute is enough to
			// tell an operator the mirror fell behind; the count is kept so
			// nothing is silently swallowed.
			e.log.Throttlef("overflow:"+r.Name, time.Minute, logx.Warn,
				"[%s] watcher queue overflowed, scheduling a full resync", r.Name)
		}
		return
	}
	subject := ev.Path
	if ev.Op&event.Move != 0 && ev.From != "" {
		subject = ev.From
	}
	r := e.lookup(subject)
	if r == nil {
		return
	}
	// A rename can cross two sync roots (mv /data/a/x /data/b/x). The event
	// carries one From and one Path, and From decides which root it lands in,
	// but the destination belongs to a different tree. Applying it as one
	// rename would make the backend join a relative path that starts with
	// ".." onto its target, i.e. write outside the destination directory.
	// Split it into a delete here and a create there.
	if ev.Op&event.Move != 0 && ev.From != "" && !watcher.IsUnder(r.Source, ev.Path) {
		e.Handle(event.Event{Path: ev.From, Op: event.Delete, IsDir: ev.IsDir, At: ev.At})
		e.Handle(event.Event{Path: ev.Path, Op: event.Create, IsDir: ev.IsDir, At: ev.At})
		return
	}
	if r.Filter != nil {
		dir := ev.IsDir
		if ev.Op&event.Move != 0 && ev.From != "" {
			// A move is interesting if either side is interesting.
			inFrom := r.Filter.Match(relPath(r.Source, ev.From), true)
			inTo := r.Filter.Match(relPath(r.Source, ev.Path), dir)
			if inFrom && inTo {
				e.bump(r, func(s *Stats) { s.Filtered++ })
				return
			}
		} else if r.Filter.Match(relPath(r.Source, subject), dir) {
			e.bump(r, func(s *Stats) { s.Filtered++ })
			return
		}
	}
	r.agg.Add(ev)
	r.mu.Lock()
	r.lastEvent = e.now()
	r.st.Events++
	r.mu.Unlock()
}

func (e *Engine) bump(r *Root, fn func(*Stats)) {
	r.mu.Lock()
	fn(&r.st)
	r.mu.Unlock()
}

// lookup returns the most specific root containing p.
//
// roots is kept sorted by descending source length (see New), so the first
// match is the longest prefix. Without this a sync of /data would swallow the
// events of a sync of /data/logs and the inner sync would never see them.
func (e *Engine) lookup(p string) *Root {
	for _, r := range e.roots {
		if watcher.IsUnder(r.Source, p) {
			return r
		}
	}
	return nil
}

func relPath(root, p string) string {
	rel := strings.TrimPrefix(p, root)
	rel = strings.TrimPrefix(rel, string(filepath.Separator))
	rel = strings.TrimPrefix(rel, "/")
	// Exclude patterns are written with '/' and matched segment by segment.
	// On Windows the watcher hands out '' separated paths, which matched
	// nothing at all: every exclude silently stopped working there.
	return filepath.ToSlash(rel)
}

// Tick closes every window whose delay elapsed. Exported so tests can drive the
// clock without sleeping.
func (e *Engine) Tick(now time.Time) {
	e.maybeFullResync(now)
	for _, r := range e.roots {
		if r.agg.Len() == 0 && !r.agg.Overflowed() {
			continue
		}
		r.mu.Lock()
		last := r.lastEvent
		r.mu.Unlock()
		// Flush when the delay elapsed, when events were lost, or when the
		// window hit its size cap: in the last case waiting any longer would
		// only make the next batch bigger for no benefit.
		if !r.gateOpen(now) {
			continue
		}
		if r.ready(now, last) {
			e.flushAsync(r)
		}
	}
}

// ready reports whether this sync's window should be transferred now.
//
// The forced conditions come first and are not negotiable: events were lost,
// the window is at its size cap, or the oldest change has waited max_hold. The
// close_write check sits after them on purpose - it may postpone a normal flush,
// never a forced one, because postponing those means either a lost-update mirror
// or an unbounded wait.
func (r *Root) ready(now, last time.Time) bool {
	if r.agg.Overflowed() || r.agg.Urgent() || r.agg.Overdue(now, r.MaxHold) {
		return true
	}
	return r.quiet(now, last)
}

// quiet reports whether the tree has been quiet for the delay and, when
// close_write is on, whether every pending file has been closed.
//
// Delay still means "quiet for this long": a close restarts the clock rather
// than flushing at once, so a writer that touches a file again a moment later
// is still folded into the same batch.
func (r *Root) quiet(now, last time.Time) bool {
	if now.Sub(last) < r.Delay {
		return false
	}
	if r.CloseWrite && r.agg.OpenFiles() > 0 {
		return false
	}
	return true
}

// gateOpen reports whether a sync that failed recently may be tried again.
//
// The engine retries inside apply, so this gate only governs how often a whole
// round is repeated when every attempt in it failed. A target that is down for
// hours must not turn into a tight rescan loop: with "full resync" in play each
// round walks the entire tree, which is both pointless and expensive.
func (r *Root) gateOpen(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retryAt.IsZero() || !now.Before(r.retryAt)
}

// maybeFullResync turns the periodic safety net into one overflow flag per
// root, which the normal flush path then handles as a full transfer.
func (e *Engine) maybeFullResync(now time.Time) {
	d := e.FullResync()
	if d <= 0 {
		return
	}
	if e.nextFull.IsZero() {
		e.nextFull = now.Add(d)
		return
	}
	if now.Before(e.nextFull) {
		return
	}
	e.nextFull = now.Add(d)
	for _, r := range e.roots {
		r.agg.MarkOverflow()
	}
	e.log.Infof("periodic full resync of %d sync(s)", len(e.roots))
}

// FlushAll synchronously drains every window. Used by tests and on shutdown.
func (e *Engine) FlushAll(ctx context.Context) {
	for _, r := range e.roots {
		e.flushSync(ctx, r)
	}
}

// flushSync runs the transfer on the calling goroutine.
func (e *Engine) flushSync(ctx context.Context, r *Root) {
	// One writer per root. A background transfer may still be finishing (that
	// is exactly the shutdown case), so wait for the flag instead of racing it.
	for {
		r.mu.Lock()
		if !r.flushing {
			r.flushing = true
			r.mu.Unlock()
			break
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
	e.apply(ctx, r)
	r.mu.Lock()
	r.flushing = false
	r.pending = false
	r.mu.Unlock()
}

// flushAsync starts a transfer in the background, respecting the process cap.
func (e *Engine) flushAsync(r *Root) {
	r.mu.Lock()
	if r.flushing {
		r.pending = true
		r.mu.Unlock()
		return
	}
	r.flushing = true
	r.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if !e.acquire(e.ctx, r) {
			r.mu.Lock()
			r.flushing = false
			r.mu.Unlock()
			return
		}
		defer e.release(r)

		e.apply(e.ctx, r)

		r.mu.Lock()
		r.flushing = false
		again := r.pending
		r.pending = false
		last := r.lastEvent
		r.mu.Unlock()
		// Events arrived while the transfer was running. Flush again only
		// once the delay elapsed or the window filled up; otherwise keep
		// accumulating so a steady stream of small writes still coalesces.
		//
		// Overflow is deliberately not in this condition even though ready()
		// has it: a batch that failed during an overflow marks the window
		// overflowed again, and flushing on that here would retry in a tight
		// loop, bypassing the retry gate that apply just armed.
		if now := e.now(); again && r.agg.Len() > 0 &&
			(r.agg.Urgent() || r.agg.Overdue(now, r.MaxHold) || r.quiet(now, last)) {
			e.flushAsync(r)
		}
	}()
}

// acquire takes a transfer slot: first the per-sync one, then the global one.
// The order matters only for fairness; either slot is always released, so a sync
// waiting on the global cap cannot starve the others.
func (e *Engine) acquire(ctx context.Context, r *Root) bool {
	if r.limit != nil {
		select {
		case r.limit <- struct{}{}:
		case <-ctx.Done():
			return false
		}
	}
	select {
	case e.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		if r.limit != nil {
			<-r.limit
		}
		return false
	}
}

func (e *Engine) release(r *Root) {
	<-e.sem
	if r.limit != nil {
		<-r.limit
	}
}

// Wait blocks until every in-flight transfer finished.
func (e *Engine) Wait() { e.wg.Wait() }

func (e *Engine) wait(ctx context.Context, d time.Duration) {
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Close stops accepting new transfers and waits for running ones.
func (e *Engine) Close() {
	e.cancel()
	e.wait(e.ctx, 30*time.Second)
}

// apply transfers one window, retrying with backoff and putting the changes
// back into the window when every attempt failed.
func (e *Engine) apply(ctx context.Context, r *Root) {
	changes, overflow := r.agg.Flush()
	if len(changes) == 0 && !overflow {
		return
	}
	if overflow {
		e.log.Warnf("[%s] window overflowed, forcing a full resync", r.Name)
	}
	batch := syncpkg.Batch{Changes: changes, Full: overflow}
	if overflow {
		batch.Reason = "watcher overflow"
	}
	t0 := e.now()
	err := e.applyRetry(ctx, r, batch)

	r.mu.Lock()
	r.st.Batches++
	if overflow {
		r.st.FullSyncs++
	} else {
		r.st.Applied += uint64(len(changes))
	}
	if err != nil {
		r.st.Errors++
		r.st.LastError = err.Error()
		r.fails++
		// 15s, 30s, 1m ... capped at ten minutes, with jitter from Backoff.
		r.retryAt = e.now().Add(syncpkg.Backoff(r.fails-1, 15*time.Second, 10*time.Minute))
	} else {
		r.st.LastFlush = t0
		r.fails = 0
		r.retryAt = time.Time{}
	}
	if p := r.agg.Stats().PeakPending; p > r.st.Peak {
		r.st.Peak = p
	}
	r.mu.Unlock()

	if err != nil {
		// Keyed by sync name: a target that is down for hours would otherwise
		// write one line per retry round and reproduce issue #93 (a single
		// stuck machine produced a 438 GB log file).
		e.log.Throttlef("failed:"+r.Name, time.Minute, logx.Error,
			"[%s] transfer failed after %d attempts: %v", r.Name, r.Retries, err)
		// Flush cleared the flag, so a failed full resync would otherwise be
		// retried as an ordinary incremental batch and the tree would stay
		// inconsistent forever.
		if overflow {
			r.agg.MarkOverflow()
		}
		e.readd(r, changes)
		return
	}
	if len(changes) > 0 {
		e.log.Debugf("[%s] applied %d changes in %s", r.Name, len(changes), e.now().Sub(t0).Round(time.Millisecond))
	}
}

func (e *Engine) applyRetry(ctx context.Context, r *Root, batch syncpkg.Batch) error {
	var last error
	for attempt := 0; attempt < r.Retries; attempt++ {
		if attempt > 0 {
			d := syncpkg.Backoff(attempt-1, time.Second, 5*time.Minute)
			e.bump(r, func(s *Stats) { s.Retries++ })
			e.log.Throttlef("retry:"+r.Name, time.Minute, logx.Warn,
				"[%s] retry %d in %s: %v", r.Name, attempt, d.Round(time.Millisecond), last)
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := r.Backend.Apply(ctx, batch); err == nil {
			return nil
		} else {
			last = err
		}
	}
	return last
}

// readd returns a failed batch to the window so the next round retries exactly
// those paths instead of restarting the whole tree.
func (e *Engine) readd(r *Root, changes []aggregate.Change) {
	for _, c := range changes {
		// Directly into the window: going through Handle would run these paths
		// by the filter and the router a second time and count them again in
		// the events counter, which made the "events" figure drift upwards
		// every time a backend hiccuped.
		//
		// Readd rather than Add so the entry keeps the "still open" answer it
		// had when the batch was taken. Rebuilding an event from the Op mask
		// would re-open files whose close had already arrived - the mask of a
		// batch carries Write but not the close state - and a close_write sync
		// would then sit on a finished file until max_hold.
		r.agg.Readd(c)
	}
	r.mu.Lock()
	r.lastEvent = e.now()
	r.mu.Unlock()
}
