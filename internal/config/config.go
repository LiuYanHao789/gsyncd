// Package config loads and validates gsyncd configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Mode selects the synchronisation backend.
type Mode string

// Supported backends.
const (
	// ModeRsync runs a local rsync binary against a local or remote target.
	ModeRsync Mode = "rsync"
	// ModeRsyncSSH runs rsync over ssh for both directions of the protocol.
	ModeRsyncSSH Mode = "rsyncssh"
	// ModeDirect copies files directly without any external program.
	ModeDirect Mode = "direct"
)

// Config is the whole program configuration.
type Config struct {
	Global Global
	Syncs  []Sync
}

// Global holds process wide settings.
type Global struct {
	LogFile        string
	LogLevel       string
	LogMaxSize     int64
	LogMaxFiles    int
	PidFile        string
	StatusFile     string
	StatusInterval time.Duration
	Nodaemon       bool
	// MaxProcesses caps concurrent backend processes across all syncs.
	MaxProcesses int
	// MaxPending caps the aggregated change window per sync.
	MaxPending int
	// WatchInterval selects the polling backend when > 0 on any platform.
	WatchInterval time.Duration
	// FullResync is the interval of a periodic wholesale resync. It is the
	// safety net for trees the kernel cannot watch completely, either because
	// /proc/sys/fs/inotify/max_user_watches is exhausted or because the
	// platform has no recursive notification API. Zero means "only when the
	// watcher reports that it could not watch everything".
	FullResync time.Duration

	// MemoryLimit is a hard ceiling on the Go heap, as a byte count or a
	// suffixed size ("1g", "512m"). Zero leaves it to the runtime.
	//
	// It matters because the per-directory tables, the pending window and the
	// walk's own buffers all scale with the tree, and an unbounded daemon on a
	// 100T tree competes with whatever else runs on the host. With a limit the
	// runtime returns memory to the kernel more eagerly and, if the live set
	// genuinely exceeds it, spends more CPU on GC instead of growing - which is
	// the trade an operator asked for by setting it. The process never dies of
	// it; only the OOM killer can do that.
	MemoryLimit int64
	// Nice is the CPU niceness gsyncd runs at, -20 to 19. Zero leaves the
	// process alone, which is right for a dedicated mirror host and wrong for
	// one that also serves java, mysql or redis: an initial copy wants every
	// core it can get, and those services are the reason the host exists.
	Nice int
	// IOPrio is the io scheduling class: "", "idle", "best-effort" or
	// "realtime", optionally with a level as in "best-effort:7". Idle is the
	// useful one on a shared disk - the kernel hands the bandwidth to every
	// other process first.
	IOPrio string
	// TargetGuard refuses to start unless each local target already exists and
	// is on a non-root mount or carries the .gsyncd-target marker. It is the
	// protection against an unmounted backup disk: without it the daemon would
	// create the mount point directory on the root filesystem and fill it.
	TargetGuard bool
}

// Sync is one source/target pair.
type Sync struct {
	Name   string
	Mode   Mode
	Source string
	// Target is either a local path (ModeDirect, ModeRsync) or
	// user@host:/path (ModeRsyncSSH, ModeRsync remote).
	Target string
	// TargetDir is used by rsyncssh where Target is only the host.
	TargetDir string

	Delay time.Duration
	// MaxHold bounds how long a change may wait for the tree to go quiet. It
	// exists for files that are never quiet: an append-only log or a database
	// file that is written to continuously produces events forever, so the
	// delay never expires and the mirror would stay at the first snapshot
	// until the writer exits (issues #77, #458). Zero keeps the historical
	// behaviour of waiting for silence.
	MaxHold time.Duration
	Delete  bool
	// CloseWrite holds a flush back until every pending file has been closed
	// by its last writer instead of copying as soon as the tree goes quiet.
	//
	// Off by default. A file that is opened once and never closed - a database
	// datafile, an mmap'd index, anything a daemon keeps a handle on - would
	// then never be copied at all, and the mirror would stay silently at its
	// old contents. Turn it on for trees whose large files are written by
	// ordinary open/write/close producers, where the default would copy a
	// half-written snapshot (issue #458). max_hold remains the bound.
	CloseWrite bool
	// DeleteMode is "", "startup" or "running". It distinguishes "mirror the
	// source exactly, always" from the two half modes lsyncd also offers.
	DeleteMode string
	// Excludes are rsync style patterns applied to the watcher.
	Excludes []string
	// RSyncExtra are literal extra rsync arguments.
	RSyncExtra []string

	// MaxProcesses is the per-sync cap; 0 means inherit the global one.
	MaxProcesses int
	// BatchLimit caps how many paths go into one rsync invocation.
	BatchLimit int
	// Fsync forces the direct backend to flush every copied file before the
	// rename. Off by default: files at or above 8 MiB are flushed anyway,
	// and flushing a million small ones costs more than the copy itself.
	Fsync bool
	// Workers is the number of concurrent copy goroutines in the direct
	// backend. 0 means min(NumCPU, 16).
	//
	// It is separate from MaxProcesses on purpose: that one throttles rsync
	// children, which cost a fork and a whole directory tree walk each, so a
	// handful is plenty. Copy goroutines are cheap and pipelined behind the
	// same filesystem queue, and on huge trees the useful range is different.
	Workers int

	// Prefetch is the number of goroutines that read directory listings ahead
	// of the initial walk. 0 means min(NumCPU, 16); a negative value turns
	// prefetching off and walks serially.
	//
	// Reading listings concurrently is what makes a cold start fast: one
	// metadata request in flight leaves the disk queue empty between requests.
	// On 210k directories the same walk took 126s serially and 15.9s with 16
	// readers. Visit order does not change, so the checkpoint stays valid.
	Prefetch int

	// Checkpoint is the path of the initial-scan checkpoint file. Empty means
	// no checkpoint: every start re-reads the whole tree, which on a 100 TB
	// mirror is hours of metadata before the first byte moves.
	//
	// The file only ever speeds things up. A missing, truncated, stale or
	// unreadable checkpoint makes the next start verify more directories, never
	// fewer, so it can be deleted at any time without consequences.
	Checkpoint string

	SSH SSH
}

// StartupDelete reports whether the initial sync may remove target entries
// that are missing on the source.
//
// lsyncd's delete = "running" exists for the case where the target starts out
// with data that must survive the first pass (a restored backup, a replica that
// is deliberately a superset), so the first sync only adds.
func (s Sync) StartupDelete() bool {
	return s.Delete && s.DeleteMode != "running"
}

// RunningDelete reports whether a delete observed while running is propagated.
// delete = "startup" is the mirror image: clean up once, then never remove.
func (s Sync) RunningDelete() bool {
	return s.Delete && s.DeleteMode != "startup"
}

// SSH holds connection details for the rsyncssh mode.
type SSH struct {
	Binary       string
	Port         int
	IdentityFile string
	Options      map[string]string
}

// Default returns the built-in defaults.
func Default() Config {
	return Config{
		Global: Global{
			LogLevel:       "info",
			LogMaxSize:     100 << 20,
			LogMaxFiles:    5,
			StatusInterval: 10 * time.Second,
			MaxProcesses:   8,
			MaxPending:     500_000,
			TargetGuard:    true,
		},
	}
}

// Load reads a configuration file. TOML is the file format.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg, err := Parse(string(data))
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	base := filepath.Dir(path)
	if err := cfg.Global.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: global: %w", path, err)
	}
	for i := range cfg.Syncs {
		if err := cfg.Syncs[i].Validate(base); err != nil {
			return Config{}, fmt.Errorf("%s: sync %q: %w", path, cfg.Syncs[i].Name, err)
		}
	}
	return cfg, nil
}

// Parse builds a Config from TOML text.
func Parse(src string) (Config, error) {
	root, err := parseTOML(src)
	if err != nil {
		return Config{}, err
	}
	cfg := Default()

	// Every key at the top level must be a known section. A key that belongs in
	// [global] but was written above it used to be dropped without a word, and
	// the process then started with no log_file and no status_file - the exact
	// silent-ignore failure this parser refuses to make elsewhere. The example
	// configuration promises "a misspelled key exits", so it has to hold here
	// too.
	for k := range root {
		switch k {
		case "global", "sync":
			continue
		}
		if isGlobalKey(k) {
			return Config{}, fmt.Errorf("top level: key %q belongs in a [global] section, not at the top of the file", k)
		}
		return Config{}, fmt.Errorf("top level: unknown key %q (expected [global] or [[sync]])", k)
	}

	if g, ok := root["global"].(tomlTable); ok {
		if err := applyGlobal(&cfg.Global, g); err != nil {
			return Config{}, err
		}
	}
	raw, _ := root["sync"].([]tomlTable)
	if len(raw) == 0 {
		return Config{}, errors.New("no [[sync]] section found")
	}
	for i, t := range raw {
		s, err := applySync(t)
		if err != nil {
			return Config{}, fmt.Errorf("sync #%d: %w", i+1, err)
		}
		cfg.Syncs = append(cfg.Syncs, s)
	}
	return cfg, nil
}

// Validate checks the settings that have no safe silent fallback.
//
// It runs on Load rather than on Parse so that a test can build a Global in
// memory, but a daemon started by systemd can never come up with a nice value
// it cannot apply: the failure would otherwise only show up as a log line
// after the first copy had already begun.
func (g Global) Validate() error {
	if g.Nice < -20 || g.Nice > 19 {
		return fmt.Errorf("nice %d is out of range, expected -20 to 19", g.Nice)
	}
	if g.IOPrio != "" {
		name := strings.ToLower(strings.TrimSpace(g.IOPrio))
		if i := strings.IndexByte(name, ':'); i >= 0 {
			lv, err := strconv.Atoi(strings.TrimSpace(name[i+1:]))
			if err != nil || lv < 0 || lv > 7 {
				return fmt.Errorf("ioprio %q: level must be 0-7", g.IOPrio)
			}
			name = strings.TrimSpace(name[:i])
		}
		switch name {
		case "idle", "best-effort", "best_effort", "besteffort", "realtime":
		default:
			return fmt.Errorf("ioprio %q: expected idle, best-effort or realtime", g.IOPrio)
		}
	}
	return nil
}

// normKey folds the spelling of a configuration key so that log_file, log-file
// and logfile all mean the same thing.
//
// README.md documents the underscored spelling everywhere while the parser used
// to accept only the run-together one, so every example copied out of the
// documentation was rejected with "global: unknown key \"log_file\"". Accepting
// both, and rejecting neither, is the only version that cannot be wrong.
func normKey(k string) string {
	k = strings.ToLower(k)
	k = strings.ReplaceAll(k, "_", "")
	return strings.ReplaceAll(k, "-", "")
}

// isGlobalKey reports whether k is one of the keys applyGlobal understands.
// Used only to turn a misplaced key into a pointed error message.
func isGlobalKey(k string) bool {
	switch normKey(k) {
	case "logfile", "loglevel", "logmaxsize", "logmaxfiles", "pidfile",
		"statusfile", "statusinterval", "nodaemon", "maxprocesses",
		"maxpending", "watchinterval", "fullresync", "memorylimit", "nice", "ioprio", "targetguard":
		return true
	}
	return false
}

func applyGlobal(g *Global, t tomlTable) error {
	for k, v := range t {
		switch normKey(k) {
		case "logfile":
			g.LogFile = asString(v)
		case "loglevel":
			g.LogLevel = asString(v)
		case "logmaxsize":
			g.LogMaxSize = asInt64(v)
		case "logmaxfiles":
			g.LogMaxFiles = int(asInt64(v))
		case "pidfile":
			g.PidFile = asString(v)
		case "statusfile":
			g.StatusFile = asString(v)
		case "statusinterval":
			g.StatusInterval = seconds(v)
		case "nodaemon":
			g.Nodaemon = asBool(v)
		case "maxprocesses":
			g.MaxProcesses = int(asInt64(v))
		case "maxpending":
			g.MaxPending = int(asInt64(v))
		case "watchinterval":
			g.WatchInterval = seconds(v)
		case "fullresync":
			g.FullResync = seconds(v)
		case "memorylimit":
			g.MemoryLimit = asBytes(v)
		case "nice":
			g.Nice = int(asInt64(v))
		case "ioprio":
			g.IOPrio = asString(v)
		case "targetguard":
			g.TargetGuard = asBool(v)
		default:
			return fmt.Errorf("global: unknown key %q", k)
		}
	}
	return nil
}

func applySync(t tomlTable) (Sync, error) {
	// direct is the default: it needs no rsync and no ssh, which is the
	// whole point of a single static binary.
	s := Sync{
		Mode:       ModeDirect,
		Delay:      15 * time.Second,
		Delete:     true,
		DeleteMode: "",
		BatchLimit: 10_000,
	}
	for k, v := range t {
		switch normKey(k) {
		case "name":
			s.Name = asString(v)
		case "mode":
			m := Mode(strings.ToLower(asString(v)))
			switch m {
			case ModeRsync, ModeRsyncSSH, ModeDirect:
				s.Mode = m
			default:
				return s, fmt.Errorf("unknown mode %q", m)
			}
		case "source":
			s.Source = asString(v)
		case "target":
			s.Target = asString(v)
		case "targetdir":
			s.TargetDir = asString(v)
		case "delay":
			s.Delay = seconds(v)
		case "maxhold", "max_hold":
			s.MaxHold = seconds(v)
		case "closewrite", "close_write":
			s.CloseWrite = asBool(v)
		case "delete":
			switch x := v.(type) {
			case bool:
				s.Delete = x
			case string:
				switch strings.ToLower(x) {
				case "true", "yes", "running":
					s.Delete, s.DeleteMode = true, "running"
				case "startup":
					s.Delete, s.DeleteMode = true, "startup"
				case "false", "no":
					s.Delete = false
				default:
					return s, fmt.Errorf("delete: unknown value %q", x)
				}
			default:
				return s, fmt.Errorf("delete: expected bool or string")
			}
		case "exclude", "excludes":
			s.Excludes = asStrings(v)
		case "rsyncopts", "rsyncextra":
			s.RSyncExtra = asStrings(v)
		case "maxprocesses":
			s.MaxProcesses = int(asInt64(v))
		case "workers":
			s.Workers = int(asInt64(v))
		case "prefetch":
			s.Prefetch = int(asInt64(v))
		case "checkpoint":
			s.Checkpoint = asString(v)
		case "batchlimit":
			s.BatchLimit = int(asInt64(v))
		case "fsync":
			s.Fsync = asBool(v)
		case "ssh":
			st, ok := v.(tomlTable)
			if !ok {
				return s, fmt.Errorf("ssh: expected table")
			}
			if err := applySSH(&s.SSH, st); err != nil {
				return s, err
			}
		default:
			return s, fmt.Errorf("unknown key %q", k)
		}
	}
	if s.Name == "" {
		s.Name = filepath.Base(strings.TrimSuffix(s.Source, "/"))
		if s.Name == "" || s.Name == "." || s.Name == "/" {
			s.Name = "sync"
		}
	}
	return s, nil
}

func applySSH(sh *SSH, t tomlTable) error {
	for k, v := range t {
		switch normKey(k) {
		case "binary":
			sh.Binary = asString(v)
		case "port":
			sh.Port = int(asInt64(v))
		case "identityfile":
			sh.IdentityFile = asString(v)
		case "options":
			st, ok := v.(tomlTable)
			if !ok {
				return fmt.Errorf("ssh.options: expected table")
			}
			sh.Options = make(map[string]string, len(st))
			for kk, vv := range st {
				sh.Options[kk] = asString(vv)
			}
		default:
			return fmt.Errorf("ssh: unknown key %q", k)
		}
	}
	return nil
}

// Validate checks the sync and makes paths absolute relative to base.
func (s *Sync) Validate(base string) error {
	if s.Source == "" {
		return errors.New("source is required")
	}
	if !filepath.IsAbs(s.Source) {
		s.Source = filepath.Join(base, s.Source)
	}
	s.Source = filepath.Clean(s.Source)
	st, err := os.Stat(s.Source)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("source %s is not a directory", s.Source)
	}
	switch s.Mode {
	case ModeRsyncSSH:
		if s.Target == "" {
			return errors.New("target (host) is required for rsyncssh")
		}
		if s.TargetDir == "" {
			s.TargetDir = s.Source
		}
	case ModeRsync, ModeDirect:
		if s.Target == "" {
			return errors.New("target is required")
		}
	}
	// A direct target inside the source is not a mistake the daemon can
	// recover from: every file it copies becomes a new source file, which
	// generates an event, which copies it again, until the disk is full. The
	// original lsyncd had the same hazard with a local rsync target and no
	// check at all, so the failure mode was a filled volume rather than an
	// error message.
	if s.Mode != ModeRsyncSSH {
		// Relative to the configuration file, like the source above. Resolving
		// it against the working directory instead would put the target
		// somewhere under / when started by systemd, and the two paths of one
		// sync would follow different rules.
		if !filepath.IsAbs(s.Target) {
			s.Target = filepath.Join(base, s.Target)
		}
		s.Target = filepath.Clean(s.Target)
		abs, err := filepath.Abs(s.Target)
		if err == nil {
			s.Target = abs
			if within(s.Source, s.Target) {
				return fmt.Errorf("target %s is inside the source; a direct copy would mirror itself forever", s.Target)
			}
			if within(s.Target, s.Source) {
				return fmt.Errorf("source %s is inside the target; deleting extras would remove the source", s.Source)
			}
		}
	}
	if s.Delay < 0 {
		return errors.New("delay must not be negative")
	}
	if s.CloseWrite && s.MaxHold < 0 {
		// "wait for a close that may never come, with no upper bound" is a
		// mirror that silently stops updating. Refusing it here means the
		// operator sees a configuration error at -check time instead of
		// discovering months later that one file never made it.
		return errors.New("close_write needs max_hold; without it a file that is never closed is never copied")
	}
	if s.BatchLimit <= 0 {
		s.BatchLimit = 10_000
	}
	return nil
}

// within reports whether p is at or below root.
func within(root, p string) bool {
	r := filepath.Clean(root)
	c := filepath.Clean(p)
	if c == r {
		return true
	}
	rel, err := filepath.Rel(r, c)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func asString(v tomlValue) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return fmt.Sprintf("%d", x)
	case bool:
		return fmt.Sprintf("%t", x)
	}
	return ""
}

func asInt64(v tomlValue) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		var n int64
		fmt.Sscanf(x, "%d", &n)
		return n
	}
	return 0
}

// asBytes parses a byte count, accepting the suffixes an operator is likely to
// write in a config file ("512m", "1g") as well as a plain number.
func asBytes(v tomlValue) int64 {
	s := strings.TrimSpace(strings.ToLower(asString(v)))
	if s == "" {
		return 0
	}
	mult := int64(1)
	switch s[len(s)-1] {
	case 'k':
		mult, s = 1<<10, s[:len(s)-1]
	case 'm':
		mult, s = 1<<20, s[:len(s)-1]
	case 'g':
		mult, s = 1<<30, s[:len(s)-1]
	case 't':
		mult, s = 1<<40, s[:len(s)-1]
	}
	n := asInt64(s)
	if n <= 0 {
		return 0
	}
	return n * mult
}

func asBool(v tomlValue) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(x, "true") || strings.EqualFold(x, "yes")
	case int64:
		return x != 0
	}
	return false
}

func seconds(v tomlValue) time.Duration {
	switch v.(type) {
	case string:
		d, err := time.ParseDuration(asString(v))
		if err == nil {
			return d
		}
	}
	return time.Duration(asInt64(v)) * time.Second
}

func asStrings(v tomlValue) []string {
	switch x := v.(type) {
	case []tomlValue:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, asString(e))
		}
		return out
	case string:
		return []string{x}
	}
	return nil
}
