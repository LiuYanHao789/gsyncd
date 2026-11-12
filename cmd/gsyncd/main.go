// Command gsyncd is a one-binary, dependency-free replacement for lsyncd.
//
// It watches one or more directory trees and mirrors them to a target. Unlike
// the original it needs no Lua interpreter, no rsync and no ssh: the default
// backend copies files itself. The optional rsync and rsyncssh backends exist
// for compatibility with existing setups.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gsyncd/internal/config"
	"gsyncd/internal/engine"
	"gsyncd/internal/filter"
	"gsyncd/internal/guard"
	"gsyncd/internal/logx"
	"gsyncd/internal/scanstate"
	"gsyncd/internal/status"
	syncpkg "gsyncd/internal/sync"
	"gsyncd/internal/verify"
	"gsyncd/internal/watcher"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "0.5.0"

// envReloaded marks the process that replaced itself for a SIGHUP reload. It
// must not daemonize again: it already has the right session and pid, and a
// second detach would orphan the pid file.
const envReloaded = "GSYNCD_RELOADED"

func main() {
	ignoreReloadSignalUntilReady()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gsyncd: %v\n", err)
		os.Exit(1)
	}
}

type flags struct {
	configPath    string
	logFile       string
	logLevel      string
	pidFile       string
	statusFile    string
	nodaemon      bool
	once          bool
	showStatus    string
	check         bool
	initTarget    bool
	verifyTree    bool
	verifyContent bool
	verifySample  int
	verifyWorkers int
	watchEvery    time.Duration
	fullResync    time.Duration
	printVer      bool
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("gsyncd", flag.ContinueOnError)
	fs.StringVar(&f.configPath, "config", "", "path to the TOML configuration file")
	fs.StringVar(&f.logFile, "logfile", "", "log file (overrides the config)")
	fs.StringVar(&f.logLevel, "loglevel", "", "debug|info|warn|error|none")
	fs.StringVar(&f.pidFile, "pidfile", "", "pid file path")
	fs.StringVar(&f.statusFile, "statusfile", "", "status file path")
	fs.BoolVar(&f.nodaemon, "nodaemon", false, "stay in the foreground")
	fs.BoolVar(&f.once, "once", false, "run the initial sync and exit")
	fs.StringVar(&f.showStatus, "status", "", "print the status file at PATH and exit")
	fs.BoolVar(&f.check, "check", false, "validate the configuration and exit")
	fs.BoolVar(&f.initTarget, "init-target", false, "write the .gsyncd-target marker into every local target and exit (the target must already exist)")
	fs.BoolVar(&f.verifyTree, "verify-tree", false, "compare the source and target directory trees and exit")
	fs.BoolVar(&f.verifyContent, "verify-content", false, "hash every selected source/target file and compare it (expensive) and exit")
	fs.IntVar(&f.verifySample, "verify-sample", 0, "with -verify-content: hash this percentage of files (0 or 100 = all)")
	fs.IntVar(&f.verifyWorkers, "verify-workers", 0, "with -verify-content: concurrent hashers (0 = default)")
	fs.DurationVar(&f.watchEvery, "watch-interval", 0, "poll interval for the portable backend (0 = default)")
	fs.DurationVar(&f.fullResync, "full-resync", -1, "periodic full resync interval (-1 = use the config)")
	fs.BoolVar(&f.printVer, "version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return f, err
	}
	return f, nil
}

func run() error {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}
	if f.printVer {
		fmt.Printf("gsyncd %s (%s)\n", version, goVersion())
		return nil
	}
	if f.showStatus != "" {
		doc, err := status.Read(f.showStatus)
		if err != nil {
			return err
		}
		return printStatus(doc)
	}
	if f.configPath == "" {
		return errors.New("missing -config")
	}

	cfg, err := config.Load(f.configPath)
	if err != nil {
		return err
	}
	if f.logFile != "" {
		cfg.Global.LogFile = f.logFile
	}
	if f.logLevel != "" {
		cfg.Global.LogLevel = f.logLevel
	}
	if f.pidFile != "" {
		cfg.Global.PidFile = f.pidFile
	}
	if f.statusFile != "" {
		cfg.Global.StatusFile = f.statusFile
	}
	if f.nodaemon {
		cfg.Global.Nodaemon = true
	}
	if f.watchEvery > 0 {
		cfg.Global.WatchInterval = f.watchEvery
	}
	if f.fullResync >= 0 {
		// The command line is an explicit operator decision, so it wins over
		// every per-sync full_resync as well.
		cfg.Global.FullResync = f.fullResync
		for i := range cfg.Syncs {
			cfg.Syncs[i].FullResync = 0
		}
	}
	// Options whose backend exists only on Linux must fail here, not turn into
	// a silent no-op that reports every entry as copied.
	for i := range cfg.Syncs {
		s := &cfg.Syncs[i]
		// These two keys are direct-backend options. A rsync/rsyncssh sync
		// ignores them, so they must not make an otherwise valid config fail
		// on a platform where the selected backend does work.
		if s.Mode != config.ModeDirect {
			continue
		}
		if s.PreserveXattrs {
			if err := requireLinuxFeature("preserve_xattrs"); err != nil {
				return fmt.Errorf("sync %q: %w", s.Name, err)
			}
		}
		if s.CopySpecial {
			if err := requireLinuxFeature("copy_special"); err != nil {
				return fmt.Errorf("sync %q: %w", s.Name, err)
			}
		}
	}
	if f.initTarget {
		// The target must already exist: creating it here would remove the
		// only signal that an unmounted filesystem is being pointed at.
		for i := range cfg.Syncs {
			s := &cfg.Syncs[i]
			if !localTarget(*s) {
				continue
			}
			if err := syncpkg.InitTarget(s.Target); err != nil {
				return fmt.Errorf("sync %q: %w", s.Name, err)
			}
			fmt.Printf("initialised target %s\n", s.Target)
		}
		return nil
	}
	if f.verifyTree || f.verifyContent {
		return runVerify(cfg, f.verifyTree, f.verifyContent, f.verifySample, f.verifyWorkers)
	}
	if f.check {
		if cfg.Global.TargetGuard {
			for i := range cfg.Syncs {
				s := &cfg.Syncs[i]
				if !localTarget(*s) {
					continue
				}
				if err := syncpkg.CheckTarget(s.Target); err != nil {
					return fmt.Errorf("sync %q: %w", s.Name, err)
				}
			}
		}
		fmt.Printf("configuration OK: %d sync(s)\n", len(cfg.Syncs))
		return nil
	}

	// Detach before the log, the pid file and the status file are touched: the
	// parent must not create any of them, or it would leave behind a pid file
	// and a status document describing a process that exits immediately.
	// nodaemon (flag or config) keeps the process in the foreground, which is
	// what a service manager wants. A one-shot run is a command, not a daemon:
	// detaching it would return the shell prompt while the copy is still
	// running and hide its exit status.
	if !cfg.Global.Nodaemon && !f.once && os.Getenv(envReloaded) == "" {
		if !daemonSupported {
			fmt.Fprintln(os.Stderr, "gsyncd: this platform cannot detach, running in the foreground")
		} else if parent, derr := daemonize(cfg.Global.LogFile); derr != nil {
			fmt.Fprintf(os.Stderr, "gsyncd: could not detach, staying in the foreground: %v\n", derr)
		} else if parent {
			return nil
		}
	}

	level, err := logx.ParseLevel(cfg.Global.LogLevel)
	if err != nil {
		return err
	}
	var logger *logx.Logger
	if cfg.Global.LogFile != "" {
		logger, err = logx.NewFile(cfg.Global.LogFile, level, cfg.Global.LogMaxSize, cfg.Global.LogMaxFiles)
		if err != nil {
			return err
		}
	} else {
		logger = logx.NewStderr(level)
	}
	defer logger.Close()

	// Apply the heap ceiling before anything large is allocated. The runtime
	// treats it as a target, not a wall: it collects harder as the live heap
	// approaches it and returns memory to the kernel instead of holding it. On
	// a 100T tree that is the difference between a neighbour service surviving
	// and the OOM killer choosing between them.
	if cfg.Global.MemoryLimit > 0 {
		debug.SetMemoryLimit(cfg.Global.MemoryLimit)
	}

	logger.Infof("gsyncd %s starting with %d sync(s)", version, len(cfg.Syncs))
	if cfg.Global.MemoryLimit > 0 {
		logger.Infof("heap limited to %d MiB", cfg.Global.MemoryLimit>>20)
	}

	// Priority is lowered before any backend starts, so that the first copy -
	// the one that can run for hours - is already behind the services the host
	// exists to serve. A kernel or container that refuses the call is logged once
	// and otherwise ignored: an unreniced mirror still mirrors.
	if pol := (guard.Policy{Nice: cfg.Global.Nice, IOPrio: cfg.Global.IOPrio}); pol.Active() {
		if err := guard.Apply(pol); err != nil {
			logger.Warnf("cannot lower priority (%s): %v", pol.Describe(), err)
		} else {
			logger.Infof("running at %s", pol.Describe())
		}
	}

	if cfg.Global.PidFile != "" {
		if err := writePidFile(cfg.Global.PidFile); err != nil {
			return err
		}
		defer os.Remove(cfg.Global.PidFile)
	}

	// Build one engine spec per configured sync.
	specs := make([]engine.Spec, 0, len(cfg.Syncs))
	for i := range cfg.Syncs {
		s := &cfg.Syncs[i]
		flt := filter.New(s.Excludes)
		be, err := buildBackend(s, cfg.Global, logger, flt)
		if err != nil {
			return fmt.Errorf("sync %q: %w", s.Name, err)
		}
		specs = append(specs, engine.Spec{
			Name:           s.Name,
			Source:         s.Source,
			Delay:          s.Delay,
			Filter:         flt,
			Backend:        be,
			MaxPending:     cfg.Global.MaxPending,
			MaxPendingHard: cfg.Global.MaxPendingHard,
			MaxHold:        maxHold(s.MaxHold),
			Retries:        3,
			MaxProcesses:   s.MaxProcesses,
			CloseWrite:     s.CloseWrite,
			DelayMode:      s.DelayMode,
			FullResync:     s.FullResync,
		})
		logger.Infof("[%s] mode=%s %s -> %s delay=%s delete=%v close_write=%v",
			s.Name, s.Mode, s.Source, s.Target, s.Delay, s.Delete, s.CloseWrite)
	}

	eng, err := engine.New(specs, engine.Options{
		MaxProcesses: cfg.Global.MaxProcesses,
		Log:          logger,
	})
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if f.once {
		if err := eng.Startup(ctx); err != nil {
			return err
		}
		logger.Infof("one-shot sync finished")
		return nil
	}

	// Signals are installed before any watch is registered. Installing 220k
	// watches takes minutes on a large tree, and the initial transfer takes
	// hours; a SIGHUP in either phase must reload, not hit the kernel default
	// disposition and terminate the daemon.
	var reloading, stopping atomic.Bool
	sig := make(chan os.Signal, 2)
	notifySignals(sig)
	go func() {
		// Keep receiving. Returning after the first signal would leave the
		// channel unread while signal.Notify keeps the default disposition
		// disabled, so a later SIGTERM would be ignored and systemd would
		// eventually SIGKILL a drain that is still in progress.
		for s := range sig {
			if isReloadSignal(s) {
				if stopping.Load() {
					continue
				}
				// Validate the new file before tearing the old process down.
				// A typo must leave the running mirror untouched, not replace
				// it with an image that exits on startup.
				if _, err := config.Load(f.configPath); err != nil {
					logger.Errorf("reload rejected, keeping the running configuration: %v", err)
					continue
				}
				// SIGHUP asks for a configuration reload. Cancelling drains
				// the engine through its normal shutdown path (in-flight
				// batches finish, the last window is flushed), and main then
				// re-executes itself with the same pid.
				logger.Infof("SIGHUP received, reloading after draining")
				reloading.Store(true)
				cancel()
				continue
			}
			// A TERM/INT during a reload cancels the restart: the operator
			// asked to stop, not to come back with the new configuration.
			if !stopping.Swap(true) {
				logger.Infof("signal %s received, shutting down", s)
			}
			reloading.Store(false)
			cancel()
		}
	}()

	// Watch first, sync second: anything created while the initial transfer
	// runs is still recorded, which closes the race the original had between
	// starting rsync and installing watches (issue #284).
	be, err := watcher.NewAuto(cfg.Global.WatchInterval, func(format string, args ...any) {
		logger.Warnf(format, args...)
	})
	if err != nil {
		return err
	}
	// The same exclude list must also stop the watch walk: a busy tree keeps
	// tens of thousands of directories that will never be synchronised, and
	// every one of them costs a kernel watch plus a slot in the watch table
	// (issue #202, #65).
	tree := watcher.New(be, watcher.Options{
		Ignore: excludeAll(cfg.Syncs, logger),
		Logf:   func(format string, args ...any) { logger.Debugf(format, args...) },
	})
	if err := checkCloseWrite(be, cfg.Syncs); err != nil {
		return err
	}
	// The status writer starts here, before a single watch is installed, and
	// not after the initial copy as it did at first. On a ten-terabyte tree
	// installing 220k watches takes minutes and the initial transfer hours;
	// those are the two phases where an operator most needs to see progress,
	// and both used to leave the file absent. What the writer reports is the
	// phase label below, because during either of them every counter that
	// measures transfer work still reads zero.
	ph := &phase{}
	ph.set(phaseWatching)
	var sw *status.Writer
	if cfg.Global.StatusFile != "" {
		sw = status.NewWriter(cfg.Global.StatusFile, cfg.Global.StatusInterval, func() status.Document {
			return buildStatus(eng, tree, cfg, ph)
		})
		sw.Start()
		defer sw.Stop()
	}

	for _, s := range cfg.Syncs {
		if ctx.Err() != nil {
			// A reload or stop arrived while installing watches. Do not spend
			// minutes finishing the rest of a tree that is about to be torn
			// down; the next process installs them from the new config.
			break
		}
		if err := tree.AddRoot(s.Source); err != nil {
			return fmt.Errorf("watch %s: %w", s.Source, err)
		}
	}
	// Taking the baseline before the initial copy guarantees that a write
	// which happens during that copy is still reported as a change.
	if p, ok := be.(interface{ Prime() }); ok {
		p.Prime()
	}
	logger.Infof("watching %d directories", tree.WatchDirCount())

	// Directories we could not register are invisible to the watcher, so the
	// only way to stay consistent is to re-verify the tree periodically. The
	// original aborted with "No space left on device" instead (issue #65).
	switch unwatched := tree.Unwatched(); {
	case cfg.Global.FullResync > 0:
		eng.SetFullResync(cfg.Global.FullResync)
		logger.Infof("periodic full resync every %s", cfg.Global.FullResync)
	case unwatched > 0:
		eng.SetFullResync(time.Hour)
		logger.Warnf("%d directories could not be watched, falling back to a full resync every hour "+
			"(set full_resync to pick a different interval)", unwatched)
	}

	// Watch the roots themselves. A root that is deleted, unmounted or
	// replaced while we run takes its watch with it and the kernel says
	// nothing the engine can act on, so without this the tree stays invisible
	// until the process restarts. The check is two syscalls per root per tick.
	go superviseRoots(ctx, tree, eng, logger, cfg.Syncs)

	ph.set(phaseInitial)
	if err := eng.Startup(ctx); err != nil {
		// A failing initial sync is not fatal: the watcher keeps running and
		// the next window retries the affected paths. This is deliberate, the
		// original exited and lost all queued events.
		logger.Errorf("initial sync incomplete: %v", err)
	}
	ph.set(phaseRunning)

	logger.Infof("entering main loop")
	err = eng.Run(ctx, tree.Events())
	tree.Close()
	if reloading.Load() && !stopping.Load() {
		logger.Infof("reloading configuration")
		// Stop the status writer before the image is replaced; buffered JSON
		// would otherwise be lost. Stop and Flush are idempotent, so the
		// deferred calls are harmless if Exec fails and this function returns
		// normally.
		if sw != nil {
			sw.Stop()
		}
		// Flush, do not close: the image is about to be replaced and a
		// draining goroutine may still log. Exec keeps the descriptor, and
		// the deferred Close covers the Exec-failure path.
		_ = logger.Flush()
		if execErr := reExecSelf(); execErr != nil {
			return fmt.Errorf("reload: %w", execErr)
		}
		return nil
	}
	if errors.Is(err, context.Canceled) {
		logger.Infof("stopped")
		return nil
	}
	return err
}

// rootCheckInterval is how often the roots are verified. It bounds how long a
// tree can stay invisible after its root is replaced, and it is deliberately
// not tied to the delay: a sync with delay = 1s would otherwise stat every
// root every second forever for no benefit.
const rootCheckInterval = 10 * time.Second

// superviseRoots keeps every sync root registered.
//
// Two independent things can make a root stop delivering events: the kernel
// drops the watch (the directory was removed or unmounted), or the directory is
// deleted and recreated, which gives the new inode a watchless life. The
// watcher reports the first as RootGone; the second it cannot see at all,
// because a recreated root has no parent watch to announce it. Both are
// repaired the same way: re-register the tree and force one full transfer, so
// whatever changed during the blind window is picked up.
func superviseRoots(ctx context.Context, tree *watcher.Tree, eng *engine.Engine, logger *logx.Logger, syncs []config.Sync) {
	t := time.NewTicker(rootCheckInterval)
	defer t.Stop()
	warnedUnwatched := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// The watch table can fill up after startup as new directories appear.
		// When that happens events under them are invisible, and the only way
		// back to consistency is the periodic wholesale resync. main() can
		// only check this once, before the first directory is created.
		if n := tree.Unwatched(); n > 0 && eng.FullResync() == 0 {
			eng.SetFullResync(time.Hour)
			if !warnedUnwatched {
				logger.Warnf("%d directories could not be watched after startup, falling back to a full resync every hour", n)
				warnedUnwatched = true
			}
		}
		for _, s := range syncs {
			src := filepath.Clean(s.Source)
			if _, err := os.Stat(src); err != nil {
				// Still gone. The engine already refuses to sync this root
				// (see Direct.walkCopy), so there is nothing to repair yet.
				continue
			}
			// Two questions, both cheap: is the directory still the one we
			// registered (same inode), and is its watch still recorded? The
			// first catches "rm -rf data && cp -a new data", where the path
			// never changes and every other signal says all is well. The
			// kernel's own notice cannot be relied on there: removing a tree
			// produces one IN_IGNORED per directory, those flood the queue,
			// and the notice for the root itself is exactly what gets dropped.
			same, err := tree.Verify(src)
			if err != nil || (same && tree.IsWatched(src)) {
				continue
			}
			if err := tree.Reattach(src); err != nil {
				logger.Errorf("[%s] cannot re-attach %s: %v", s.Name, src, err)
				continue
			}
			logger.Warnf("[%s] re-attached %s after losing its watch, forcing a full resync", s.Name, src)
			eng.Reattach(s.Source)
		}
	}
}

// excludeAll builds one ignore predicate for the watcher out of every sync's
// exclude list. A path is skipped when it is excluded by the sync that owns it;
// paths outside every sync root are left alone.
func excludeAll(syncs []config.Sync, logger *logx.Logger) func(string) bool {
	type entry struct {
		src string
		f   *filter.Filter
	}
	ents := make([]entry, 0, len(syncs))
	for _, s := range syncs {
		if f := filter.New(s.Excludes); !f.Empty() {
			ents = append(ents, entry{src: s.Source, f: f})
		}
	}
	if len(ents) == 0 {
		return nil
	}
	return func(path string) bool {
		for _, e := range ents {
			if !watcher.IsUnder(e.src, path) {
				continue
			}
			rel := strings.TrimPrefix(path, e.src)
			rel = strings.TrimPrefix(strings.TrimPrefix(rel, string(filepath.Separator)), "/")
			if rel == "" {
				continue
			}
			if e.f.Match(filepath.ToSlash(rel), true) {
				logger.Debugf("excluding %s", path)
				return true
			}
		}
		return false
	}
}

// maxHold turns the configured value into what the engine expects: zero means
// "use the default", and an explicit negative value means "wait for silence,
// however long that takes".
func maxHold(d time.Duration) time.Duration {
	if d == 0 {
		return engine.DefaultMaxHold
	}
	return d
}

// runVerify implements -verify-tree and -verify-content. Both are read-only:
// they never write to the target and never need the guard marker.
func runVerify(cfg config.Config, structural, content bool, sample, workers int) error {
	if sample < 0 {
		sample = 0
	}
	if sample > 100 {
		sample = 100
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers > 16 {
			workers = 16
		}
	}
	bad := 0
	for i := range cfg.Syncs {
		s := &cfg.Syncs[i]
		if !localTarget(*s) {
			fmt.Printf("[%s] skipped: remote target\n", s.Name)
			continue
		}
		flt := filter.New(s.Excludes)
		ignore := func(rel string, dir bool) bool { return flt.Match(rel, dir) }
		if structural {
			res, err := verify.Tree(s.Source, s.Target, verify.Options{Ignore: ignore})
			if err != nil {
				return fmt.Errorf("[%s] verify-tree: %w", s.Name, err)
			}
			fmt.Printf("[%s] tree checked=%d missing=%d extra=%d type_mismatch=%d\n",
				s.Name, res.Checked, res.Missing, res.Extra, res.TypeMismatch)
			for _, line := range res.Samples {
				fmt.Printf("  %s\n", line)
			}
			if !res.OK() {
				bad++
			}
		}
		if content {
			res, err := verify.Content(s.Source, s.Target, verify.ContentOptions{
				Ignore: ignore, Workers: workers, SamplePercent: sample,
			})
			if err != nil {
				return fmt.Errorf("[%s] verify-content: %w", s.Name, err)
			}
			fmt.Printf("[%s] content checked=%d missing=%d type_mismatch=%d hash_mismatch=%d mtime_mismatch=%d\n",
				s.Name, res.Checked, res.Missing, res.TypeMismatch, res.Mismatch, res.MtimeMismatch)
			for _, line := range res.Samples {
				fmt.Printf("  %s\n", line)
			}
			if !res.OK() {
				bad++
			}
		}
	}
	if bad > 0 {
		return fmt.Errorf("verification found differences in %d sync(s)", bad)
	}
	fmt.Println("verification OK")
	return nil
}

// localTarget reports whether the target is a filesystem path this process can
// stat. rsyncssh and rsync's host:path form name a remote endpoint instead.
func localTarget(s config.Sync) bool {
	if s.Mode == config.ModeRsyncSSH {
		return false
	}
	if s.Mode == config.ModeRsync && s.Remote {
		return false
	}
	return true
}

func buildBackend(s *config.Sync, g config.Global, logger *logx.Logger, flt *filter.Filter) (syncpkg.Backend, error) {
	logf := func(level string, format string, args ...any) {
		switch level {
		case "debug":
			logger.Debugf("["+s.Name+"] "+format, args...)
		case "warn":
			logger.Warnf("["+s.Name+"] "+format, args...)
		case "error":
			logger.Errorf("["+s.Name+"] "+format, args...)
		default:
			logger.Infof("["+s.Name+"] "+format, args...)
		}
	}
	switch s.Mode {
	case config.ModeDirect:
		d := syncpkg.NewDirect(s.Source, s.Target, s.Delete, s.Workers, logf)
		d.DeleteOnStartup = s.StartupDelete()
		d.DeleteOnRun = s.RunningDelete()
		d.Fsync = s.Fsync
		d.Prefetch = s.Prefetch
		d.TargetGuard = g.TargetGuard
		d.Verify = s.Verify
		d.VerifyRetries = s.VerifyRetries
		d.PreserveOwner = s.PreserveOwner
		d.FullMode = s.PreserveMode != "basic"
		d.SymlinkMode = s.SymlinkMode
		d.PreserveXattrs = s.PreserveXattrs
		d.PreserveHardlinks = s.PreserveHardlinks
		d.Sparse = s.Sparse
		d.CopySpecial = s.CopySpecial
		d.Bandwidth = s.BandwidthLimit
		if !flt.Empty() {
			d.Ignore = flt.Match
		}
		if cp := checkpointPath(s, g); cp != "" {
			// Every option that changes what ends up in the target belongs in
			// the fingerprint. symlink_mode and copy_special change which
			// entries exist at all; the metadata options change what a
			// directory record is allowed to skip.
			fp := scanstate.Fingerprint(s.Source, s.Target, s.Excludes,
				string(s.Mode), s.SymlinkMode, s.PreserveMode, s.PreserveOwner,
				strconv.FormatBool(s.PreserveXattrs),
				strconv.FormatBool(s.PreserveHardlinks),
				strconv.FormatBool(s.Sparse),
				strconv.FormatBool(s.CopySpecial),
				strconv.FormatBool(s.Delete), s.DeleteMode,
			)
			d.UseCheckpoint(scanstate.Open(cp, fp, logf))
		}
		return d, nil
	case config.ModeRsync:
		r := syncpkg.NewRSync(s.Source, s.Target, s.Delete, s.RSyncExtra, logf)
		r.DeleteOnStartup = s.StartupDelete()
		r.DeleteOnRun = s.RunningDelete()
		r.BatchLimit = s.BatchLimit
		r.Excludes = s.Excludes
		r.TargetGuard = g.TargetGuard
		// mode = "rsync" can name a remote target just as rsyncssh does. The
		// distinction decides whether a runtime delete is a local RemoveAll or
		// a remote --delete-missing-args, so it must not be inferred again
		// inside the backend.
		r.Remote = s.Remote
		applyRSyncOptions(r, s)
		r.OnLine = func(line string) { logger.Debugf("[%s] rsync: %s", s.Name, line) }
		return r, nil
	case config.ModeRsyncSSH:
		host := s.Target
		dir := s.TargetDir
		if s.MaxProcesses > 1 {
			logger.Warnf("[%s] rsyncssh with max_processes=%d: concurrent remote moves and rsyncs against the same destination are not safe; use 1 unless you have verified the workload", s.Name, s.MaxProcesses)
		}
		r := syncpkg.NewRSync(s.Source, host, s.Delete, s.RSyncExtra, logf)
		r.SSH = true
		r.Remote = true
		r.TargetDir = dir
		applyRSyncOptions(r, s)
		r.DeleteOnStartup = s.StartupDelete()
		r.DeleteOnRun = s.RunningDelete()
		r.BatchLimit = s.BatchLimit
		r.Excludes = s.Excludes
		r.SSHOpts = syncpkg.SSHOptions{
			Binary:       s.SSH.Binary,
			Port:         s.SSH.Port,
			IdentityFile: s.SSH.IdentityFile,
		}
		for k, v := range s.SSH.Options {
			r.SSHOpts.Extra = append(r.SSHOpts.Extra, "-o", k+"="+v)
		}
		r.OnLine = func(line string) { logger.Debugf("[%s] rsync: %s", s.Name, line) }
		return r, nil
	}
	return nil, fmt.Errorf("unknown mode %q", s.Mode)
}

// applyRSyncOptions copies the structured rsync configuration into the
// backend. It is shared by mode = "rsync" and mode = "rsyncssh" so the two
// dialects cannot drift apart in which flags they honour.
func applyRSyncOptions(r *syncpkg.RSync, s *config.Sync) {
	r.Binary = s.RSyncBinary
	r.BWLimit = s.RSyncBWLimit
	if r.BWLimit == 0 {
		// bandwidth_limit is the common key; rsync_bwlimit is the override for
		// a sync that needs a different cap than its direct counterpart.
		r.BWLimit = s.BandwidthLimit
	}
	r.Compress = s.RSyncCompress
	r.Xattrs = s.RSyncXattrs
	r.ACLs = s.RSyncACLs
	r.HardLinks = s.RSyncHardLinks
	r.Chown = s.RSyncChown
	r.Chmod = s.RSyncChmod
	r.PasswordFile = s.RSyncPasswordFile
	r.RsyncPath = s.RSyncPath
	r.DeleteExcluded = s.RSyncDeleteExcluded
	r.SymlinkMode = s.SymlinkMode
}

// checkpointPath resolves the sync's checkpoint setting to a file path.
//
// Empty disables the checkpoint. "auto" puts it next to the status file, then
// the log file, and finally in the temporary directory, so that the common
// single-machine setup gets the fast restart without any extra configuration.
// The name carries the sync name because one directory can hold several.
func checkpointPath(s *config.Sync, g config.Global) string {
	switch s.Checkpoint {
	case "":
		return ""
	case "auto":
		dir := ""
		for _, p := range []string{g.StatusFile, g.LogFile, g.PidFile} {
			if p != "" {
				dir = filepath.Dir(p)
				break
			}
		}
		if dir == "" {
			dir = os.TempDir()
		}
		return filepath.Join(dir, "gsyncd-"+s.Name+".scan")
	}
	return s.Checkpoint
}

// Phase labels reported in the status document. See the phase type.
const (
	phaseWatching = "watching"
	phaseInitial  = "initial"
	phaseRunning  = "running"
)

func buildStatus(eng *engine.Engine, tree *watcher.Tree, cfg config.Config, ph *phase) status.Document {
	doc := status.Document{
		Watches:   tree.WatchDirCount(),
		Unwatched: tree.Unwatched(),
		LostRoots: eng.LostRoots(),
		PID:       os.Getpid(),
		Phase:     ph.get(),
	}
	if d := eng.FullResync(); d > 0 {
		doc.FullResync = d.String()
	}
	doc.RSSKB, doc.HeapKB = procMemory()
	byName := map[string]config.Sync{}
	for _, s := range cfg.Syncs {
		byName[s.Name] = s
	}
	for _, r := range eng.Roots() {
		st := r.Stats()
		s := byName[r.Name]
		fr := s.FullResync
		if fr <= 0 {
			fr = cfg.Global.FullResync
		}
		if fr <= 0 {
			// The engine may have armed the automatic hourly fallback after
			// the watch table filled up; the status file has to show that.
			fr = eng.FullResync()
		}
		frText := ""
		if fr > 0 {
			frText = fr.String()
		}
		doc.Syncs = append(doc.Syncs, status.SyncState{
			Name:       r.Name,
			Source:     r.Source,
			Target:     s.Target,
			Backend:    r.Backend.Name(),
			Pending:    st.Pending,
			Batches:    st.Batches,
			Events:     st.Events,
			Filtered:   st.Filtered,
			Applied:    st.Applied,
			Files:      st.Files,
			Deletes:    st.Deletes,
			Errors:     st.Errors,
			Retries:    st.Retries,
			FullSyncs:  st.FullSyncs,
			Overflows:  st.Overflows,
			RootGone:   st.RootGone,
			Starting:   st.Starting,
			Lost:       st.RootGone > 0 && !tree.IsWatched(r.Source),
			LastError:  st.LastError,
			LastFlush:  st.LastFlush,
			Peak:       st.Peak,
			Open:       st.Open,
			CloseWrite: s.CloseWrite,
			FullResync: frText,
		})
	}
	return doc
}

// checkCloseWrite refuses close_write on a backend that cannot report closes.
//
// Without it the option would silently degrade to "flush at max_hold only": the
// poll backend sees that a file changed, never that a writer closed it, so every
// write would look permanently open. An operator who asked for "wait until it is
// written" and got "wait up to max_hold" has no way to notice from the logs.
func checkCloseWrite(be watcher.Backend, syncs []config.Sync) error {
	rep, ok := be.(watcher.CloseReporter)
	if !ok || rep.ReportsClose() {
		return nil
	}
	for _, s := range syncs {
		if s.CloseWrite {
			return fmt.Errorf("sync %q: close_write needs a watcher that reports file closes, and this backend does not", s.Name)
		}
	}
	return nil
}

func printStatus(d status.Document) error {
	fmt.Printf("gsyncd status: pid=%d uptime=%ds watched_dirs=%d phase=%s updated=%s\n",
		d.PID, d.UptimeSec, d.Watches, d.Phase, d.Updated.Format(time.RFC3339))
	if d.Unwatched > 0 {
		fmt.Printf("  warning: %d directories could not be watched (events there are invisible); "+
			"full_resync=%s\n", d.Unwatched, d.FullResync)
	} else if d.FullResync != "" {
		fmt.Printf("  periodic full_resync=%s\n", d.FullResync)
	}
	switch d.Phase {
	case phaseWatching:
		fmt.Printf("  installing kernel watches; no events are being delivered yet\n")
	case phaseInitial:
		fmt.Printf("  initial transfer in progress; the per-sync counters stay at zero until it finishes\n")
	}
	if len(d.LostRoots) > 0 {
		fmt.Printf("  warning: watch lost on %s (the tree is not being observed)\n",
			strings.Join(d.LostRoots, ", "))
	}
	for _, s := range d.Syncs {
		fmt.Printf("  [%s] %s -> %s (%s)\n", s.Name, s.Source, s.Target, s.Backend)
		fmt.Printf("    pending=%d batches=%d events=%d applied=%d files=%d deletes=%d errors=%d retries=%d full=%d overflow=%d root_gone=%d\n",
			s.Pending, s.Batches, s.Events, s.Applied, s.Files, s.Deletes, s.Errors, s.Retries, s.FullSyncs, s.Overflows, s.RootGone)
		if s.CloseWrite && s.Open > 0 {
			fmt.Printf("    %d pending file(s) still open for writing; close_write holds the window until they close or max_hold expires\n", s.Open)
		}
		if s.Starting {
			fmt.Printf("    initial sync still running: the counters below stay at zero until it finishes\n")
		}
		if s.Lost {
			fmt.Printf("    watch lost: nothing under %s is being observed\n", s.Source)
		}
		if s.LastError != "" {
			fmt.Printf("    last error: %s\n", s.LastError)
		}
	}
	return nil
}

// writePidFile records this process id, refusing to run next to a live one.
//
// Two gsyncd processes pointed at the same target is not a matter of duplicated
// work: both write, both delete "extras", and they race on the same temporary
// files, so the mirror can end up holding a mix of two different source states
// with nothing in the log to say why. The kernel gives us a free check for that
// - a pid file whose process is still alive means somebody is already there.
//
// A pid file left behind by a SIGKILL or a power cut names a process that no
// longer exists (or, worse, one that was recycled for something unrelated), so
// only a live pid blocks startup; the stale file is replaced.
func writePidFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if data, err := os.ReadFile(path); err == nil {
		if other, perr := strconv.Atoi(strings.TrimSpace(string(data))); perr == nil && other != os.Getpid() && pidAlive(other) {
			return fmt.Errorf("another gsyncd is already running with pid %d (pid file %s); "+
				"remove the file if that is wrong", other, path)
		}
	}
	return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644)
}

func goVersion() string { return strings.TrimPrefix(runtimeVersion(), "go") }
