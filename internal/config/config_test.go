package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const sample = `
[global]
log_level = "debug"
max_processes = 4
status_interval = "5s"

[[sync]]
source = "/data/a"
target = "/backup/a"
delay = 2
exclude = ["*.tmp", "/cache/"]

[[sync]]
name = "remote"
mode = "rsyncssh"
source = "/data/b"
target = "backup@host"
target_dir = "/srv/b"
delete = "startup"

[sync.ssh]
port = 2222
identity_file = "/home/u/.ssh/id_ed25519"
`

func TestParseSample(t *testing.T) {
	cfg, err := Parse(sample)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Global.LogLevel != "debug" || cfg.Global.MaxProcesses != 4 || cfg.Global.StatusInterval != 5*time.Second {
		t.Fatalf("global: %+v", cfg.Global)
	}
	if len(cfg.Syncs) != 2 {
		t.Fatalf("syncs: %d", len(cfg.Syncs))
	}
	a := cfg.Syncs[0]
	if a.Source != "/data/a" || a.Target != "/backup/a" || a.Delay != 2*time.Second {
		t.Fatalf("sync0: %+v", a)
	}
	// Without an explicit mode the built-in direct backend is used, so a
	// stock config works on a machine with no rsync installed.
	if a.Mode != ModeDirect {
		t.Fatalf("default mode = %q, want %q", a.Mode, ModeDirect)
	}
	if len(a.Excludes) != 2 || a.Excludes[0] != "*.tmp" {
		t.Fatalf("excludes: %+v", a.Excludes)
	}
	if a.Name != "a" {
		t.Fatalf("default name: %q", a.Name)
	}
	b := cfg.Syncs[1]
	if b.Mode != ModeRsyncSSH || b.TargetDir != "/srv/b" || !b.Delete || b.DeleteMode != "startup" {
		t.Fatalf("sync1: %+v", b)
	}
	if b.SSH.Port != 2222 || b.SSH.IdentityFile == "" {
		t.Fatalf("ssh: %+v", b.SSH)
	}
}

// delete = "startup" cleans the target once and then stops removing;
// delete = "running" leaves a pre-populated target alone on the first pass.
// Both were silently equivalent to delete = true before, which on a target
// that is deliberately a superset is data loss.
func TestDeleteModes(t *testing.T) {
	cases := []struct {
		raw     string
		startup bool
		running bool
	}{
		{"", true, true},
		{`"startup"`, true, false},
		{`"running"`, false, true},
		{"false", false, false},
	}
	for _, c := range cases {
		src := "[[sync]]\nsource='/a'\ntarget='/b'\n"
		if c.raw != "" {
			src += "delete = " + c.raw + "\n"
		}
		cfg, err := Parse(src)
		if err != nil {
			t.Fatalf("%q: %v", c.raw, err)
		}
		s := cfg.Syncs[0]
		if s.StartupDelete() != c.startup || s.RunningDelete() != c.running {
			t.Fatalf("%q: startup=%v running=%v, want %v/%v",
				c.raw, s.StartupDelete(), s.RunningDelete(), c.startup, c.running)
		}
	}
}

// fsync is per sync, not global: a bulk archive may want it while a hot
// directory cannot afford it.
func TestFsyncKey(t *testing.T) {
	cfg, err := Parse("[[sync]]\nsource='/a'\ntarget='/b'\nfsync = true\n")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Syncs[0].Fsync {
		t.Fatal("fsync = true was ignored")
	}
	cfg, err = Parse("[[sync]]\nsource='/a'\ntarget='/b'\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Syncs[0].Fsync {
		t.Fatal("fsync must default to off")
	}
}

// workers is a separate knob from max_processes: the first sizes the copy
// pool, the second caps rsync children, and a config that sets one must not
// silently change the other.
func TestWorkersKey(t *testing.T) {
	cfg, err := Parse("[[sync]]\nsource='/a'\ntarget='/b'\nworkers = 4\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Syncs[0].Workers; got != 4 {
		t.Fatalf("workers = 4 parsed as %d", got)
	}
	if got := cfg.Syncs[0].MaxProcesses; got != 0 {
		t.Fatalf("workers must not touch max_processes, got %d", got)
	}
	cfg, err = Parse("[[sync]]\nsource='/a'\ntarget='/b'\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Syncs[0].Workers; got != 0 {
		t.Fatalf("workers must default to 0 (auto), got %d", got)
	}
}

// A target inside the source makes every copy a new source file: infinite
// self-replication until the disk fills. A source inside the target is worse,
// because delete = true then removes the source itself.
func TestValidateRejectsOverlappingPaths(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		source string
		target string
		want   string
	}{
		{"target inside source", src, filepath.Join(src, "mirror"), "inside the source"},
		{"source inside target", filepath.Join(src, "sub"), src, "inside the target"},
	}
	for _, c := range cases {
		s := Sync{Name: "x", Source: c.source, Target: c.target, Mode: ModeDirect}
		err := s.Validate(root)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: want error containing %q, got %v", c.name, c.want, err)
		}
	}
	// A sibling target is the normal case and must pass.
	s := Sync{Name: "x", Source: src, Target: filepath.Join(root, "dst"), Mode: ModeDirect}
	if err := s.Validate(root); err != nil {
		t.Fatalf("sibling target: %v", err)
	}
}

func TestParseRejectsUnknownKey(t *testing.T) {
	_, err := Parse("[global]\nbogus = 1\n[[sync]]\nsource='/a'\ntarget='/b'\n")
	if err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("want unknown key error, got %v", err)
	}
}

func TestParseNoSync(t *testing.T) {
	if _, err := Parse("[global]\nlog_level='info'\n"); err == nil {
		t.Fatal("want an error for a config without syncs")
	}
}

func TestDurationForms(t *testing.T) {
	cases := map[string]time.Duration{"3": 3 * time.Second, "\"90s\"": 90 * time.Second, "\"2m\"": 2 * time.Minute}
	for in, want := range cases {
		cfg, err := Parse("[global]\nstatus_interval = " + in + "\n[[sync]]\nsource='/a'\ntarget='/b'\n")
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if cfg.Global.StatusInterval != want {
			t.Fatalf("%s: got %s want %s", in, cfg.Global.StatusInterval, want)
		}
	}
}

func TestMultiLineArrays(t *testing.T) {
	// A long exclude list is the reason this exists; TOML allows the array to
	// span lines and the hand-rolled parser has to fold it back together.
	src := `
[global]
nodaemon = true

[[sync]]
source = "/data/a"
target = "/backup/a"
exclude = [
  "*.tmp",   # editor droppings
  "/cache/"
]
rsync_extra = [ "-aHAX", "--numeric-ids" ]
`
	cfg, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Global.Nodaemon {
		t.Fatal("nodaemon lost after the multi-line array")
	}
	if len(cfg.Syncs) != 1 {
		t.Fatalf("syncs: %d", len(cfg.Syncs))
	}
	ex := cfg.Syncs[0].Excludes
	if len(ex) != 2 || ex[0] != "*.tmp" || ex[1] != "/cache/" {
		t.Fatalf("excludes: %+v", ex)
	}
	if got := cfg.Syncs[0].RSyncExtra; len(got) != 2 || got[0] != "-aHAX" || got[1] != "--numeric-ids" {
		t.Fatalf("rsync_extra: %+v", got)
	}
}

// TestDocumentedKeysParse guards the spelling documented in README.md and
// examples/. Both spellings have to work: the parser used to accept only the
// run-together form ("logfile"), so every copy-pasted example failed with
// 'global: unknown key "log_file"' and the program would not start at all.
// A [global] key written above the section header used to be dropped in
// silence: the process then ran with no log_file and no status_file while the
// configuration looked fine. That is worse than refusing to start.
func TestTopLevelGlobalKeysAreRejected(t *testing.T) {
	src := `
log_file = "/tmp/x.log"
status_file = "/tmp/x.json"

[[sync]]
name = "a"
source = "/s"
target = "/t"
`
	_, err := Parse(src)
	if err == nil {
		t.Fatal("a [global] key at the top level was accepted")
	}
	if !strings.Contains(err.Error(), "[global]") {
		t.Fatalf("error should point at the missing section, got: %v", err)
	}
}

// A typo at the top level must not be mistaken for a section either.
func TestUnknownTopLevelKeyIsRejected(t *testing.T) {
	src := `
[loball]
log_file = "/tmp/x.log"

[[sync]]
name = "a"
source = "/s"
target = "/t"
`
	if _, err := Parse(src); err == nil {
		t.Fatal("an unknown top level section was accepted")
	}
}

func TestDocumentedKeysParse(t *testing.T) {
	const doc = `
[global]
log_file     = "/var/log/gsyncd.log"
log_level    = "info"
log_max_size = 1048576
log_max_files = 3
status_file  = "/var/lib/gsyncd/status.json"
status_interval = "5s"
pid_file     = "/run/gsyncd.pid"
max_processes = 8
max_pending  = 1000
watch_interval = "30s"
full_resync  = "1h"
nodaemon     = true
nice         = 10
io_prio      = "idle"

[[sync]]
name     = "data"
source   = "/srv/data"
target   = "/backup/data"
mode     = "direct"
delay    = 15
delete   = "startup"
excludes = ["*.tmp"]
workers  = 4
prefetch = 12
batch_limit = 500
fsync    = false
checkpoint = "auto"

[[sync]]
name       = "remote"
mode       = "rsyncssh"
source     = "/srv/b"
target     = "backup@host"
target_dir = "/srv/b"
rsync_extra = ["--numeric-ids"]

[sync.ssh]
port          = 2222
identity_file = "/root/.ssh/id_rsa"
`
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("documented configuration rejected: %v", err)
	}
	g := cfg.Global
	if g.LogFile != "/var/log/gsyncd.log" || g.LogMaxSize != 1048576 || g.LogMaxFiles != 3 {
		t.Fatalf("log settings: %+v", g)
	}
	if g.StatusFile != "/var/lib/gsyncd/status.json" || g.StatusInterval != 5*time.Second {
		t.Fatalf("status settings: %+v", g)
	}
	if g.PidFile != "/run/gsyncd.pid" || g.MaxProcesses != 8 || g.MaxPending != 1000 {
		t.Fatalf("process settings: %+v", g)
	}
	if g.WatchInterval != 30*time.Second || g.FullResync != time.Hour || !g.Nodaemon {
		t.Fatalf("watch settings: %+v", g)
	}
	if g.Nice != 10 || g.IOPrio != "idle" {
		t.Fatalf("guard settings: nice=%d ioprio=%q", g.Nice, g.IOPrio)
	}
	if len(cfg.Syncs) != 2 {
		t.Fatalf("syncs: %d", len(cfg.Syncs))
	}
	a := cfg.Syncs[0]
	if a.BatchLimit != 500 || a.Workers != 4 || a.DeleteMode != "startup" || !a.Delete {
		t.Fatalf("sync0: %+v", a)
	}
	if a.Prefetch != 12 || a.Checkpoint != "auto" {
		t.Fatalf("sync0 prefetch/checkpoint: prefetch=%d checkpoint=%q", a.Prefetch, a.Checkpoint)
	}
	b := cfg.Syncs[1]
	if b.TargetDir != "/srv/b" || len(b.RSyncExtra) != 1 || b.SSH.Port != 2222 {
		t.Fatalf("sync1: %+v", b)
	}
	if b.SSH.IdentityFile != "/root/.ssh/id_rsa" {
		t.Fatalf("identity file: %q", b.SSH.IdentityFile)
	}
}

// TestKeySpellingsAreEquivalent pins the normalisation itself, so a future key
// cannot quietly become single-spelling again.
func TestKeySpellingsAreEquivalent(t *testing.T) {
	for _, k := range []string{"log_file", "log-file", "logfile", "LOG_FILE", "Log_File"} {
		if got := normKey(k); got != "logfile" {
			t.Fatalf("normKey(%q) = %q, want logfile", k, got)
		}
	}
}

// TestGlobalValidate pins the values that are refused instead of ignored. A
// nice value outside -20..19 or an io class the kernel has never heard of would
// otherwise be applied as a no-op and only ever show up in a log line.
func TestGlobalValidate(t *testing.T) {
	ok := []Global{
		{},
		{Nice: 19},
		{Nice: -20},
		{IOPrio: "idle"},
		{IOPrio: "best-effort:7"},
		{IOPrio: "realtime:0"},
	}
	for _, g := range ok {
		if err := g.Validate(); err != nil {
			t.Errorf("%+v rejected: %v", g, err)
		}
	}
	bad := []Global{
		{Nice: 20},
		{Nice: -21},
		{IOPrio: "turbo"},
		{IOPrio: "best-effort:8"},
		{IOPrio: "best-effort:x"},
	}
	for _, g := range bad {
		if err := g.Validate(); err == nil {
			t.Errorf("%+v accepted", g)
		}
	}
}

// memory_limit is the one knob that keeps a 100T tree from competing with the
// services the host actually exists for, so both the plain and the suffixed
// forms have to parse.
func TestMemoryLimitParsesSizes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"memory_limit = 0", 0},
		{"memory_limit = 536870912", 512 << 20},
		{"memory_limit = \"512m\"", 512 << 20},
		{"memory_limit = \"2g\"", 2 << 30},
		{"memory_limit = \"1G\"", 1 << 30},
		{"memory_limit = \"64k\"", 64 << 10},
	} {
		cfg, err := Parse("[global]\n" + tc.in + "\n\n[[sync]]\nsource = \"/a\"\ntarget = \"/b\"\n")
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if cfg.Global.MemoryLimit != tc.want {
			t.Errorf("%s: got %d want %d", tc.in, cfg.Global.MemoryLimit, tc.want)
		}
	}
}

// close_write is the opt-in from issue #458: wait for the writer to close the
// file instead of waiting for quiet. Both spellings must land on the same field,
// and it must stay off when unset - a silent default flip would stop every
// database datafile in an existing deployment from ever being copied.
func TestCloseWriteKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"close_write = true", true},
		{"closewrite = true", true},
		{"close_write = false", false},
		{"close_write = \"true\"", true},
	}
	for _, c := range cases {
		cfg, err := Parse("[global]\n[[sync]]\nsource='/a'\ntarget='/b'\n" + c.key + "\n")
		if err != nil {
			t.Fatalf("%s: %v", c.key, err)
		}
		if got := cfg.Syncs[0].CloseWrite; got != c.want {
			t.Fatalf("%s: CloseWrite=%v, want %v", c.key, got, c.want)
		}
	}
	cfg, err := Parse("[global]\n[[sync]]\nsource='/a'\ntarget='/b'\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Syncs[0].CloseWrite {
		t.Fatal("close_write is on by default")
	}
}

// close_write without the max_hold backstop is the one combination that can
// never converge: a file that is opened and never closed is never copied, and
// nothing in the status output says so. It is refused at load time.
func TestCloseWriteRejectsDisabledMaxHold(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	s := Sync{Name: "x", Source: src, Target: filepath.Join(root, "dst"), Mode: ModeDirect, CloseWrite: true, MaxHold: -1}
	err := s.Validate(root)
	if err == nil || !strings.Contains(err.Error(), "max_hold") {
		t.Fatalf("want an error naming max_hold, got %v", err)
	}
	s.MaxHold = 0
	if err := s.Validate(root); err != nil {
		t.Fatalf("close_write with the default max_hold must be accepted: %v", err)
	}
}
