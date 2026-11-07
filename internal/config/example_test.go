package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestShippedExampleParses parses the example configuration that README tells
// operators to copy. Nothing else did: the file is documentation, so a key that
// drifts out of the parser's vocabulary, or a bracket that eats an array
// element, would only be discovered by the operator who pasted it. Parse is
// used rather than Load because Load stats the source directory, and the
// example deliberately points at /srv/data.
func TestShippedExampleParses(t *testing.T) {
	cfg := parseExample(t)
	if len(cfg.Syncs) != 1 {
		t.Fatalf("only scenario A is live in the example; found %d sync(s)", len(cfg.Syncs))
	}
	s := cfg.Syncs[0]
	if s.Name != "data" || s.Mode != ModeDirect {
		t.Errorf("sync = %q/%q, want data/direct", s.Name, s.Mode)
	}
	if s.Source != "/srv/data" || s.Target != "/backup/data" {
		t.Errorf("paths = %q -> %q", s.Source, s.Target)
	}
	if s.Delay != 15*time.Second {
		t.Errorf("delay = %v, want 15s", s.Delay)
	}
	// delete = true is the boolean form: both the startup and the running
	// pass are armed, and DeleteMode stays empty to say "no restriction".
	if !s.Delete || s.DeleteMode != "" || !s.StartupDelete() || !s.RunningDelete() {
		t.Errorf("delete = %v/%q, startup=%v running=%v", s.Delete, s.DeleteMode, s.StartupDelete(), s.RunningDelete())
	}
	if len(s.Excludes) != 7 {
		t.Errorf("excludes = %q, want the seven documented patterns", s.Excludes)
	}
	if cfg.Global.StatusFile == "" {
		t.Error("example lost its status_file, which the README calls strongly recommended")
	}
}

// TestCommentedScenariosParse uncomments the rsyncssh and rsync blocks the same
// way an operator would and parses the result. Scenario A proves the live file
// is valid; this proves the two blocks a user copies next are valid too,
// including the [sync.ssh] and [sync.ssh.options] sub-tables, which no shipped
// file exercises at all.
func TestCommentedScenariosParse(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "gsyncd.example.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	src := uncommentSyncBlocks(string(data))
	if strings.Count(src, "[[sync]]") != 2 {
		t.Fatalf("expected two commented-out scenarios, extracted %d:\n%s", strings.Count(src, "[[sync]]"), src)
	}
	cfg, err := Parse(src)
	if err != nil {
		t.Fatalf("the commented-out scenarios do not parse once uncommented: %v", err)
	}
	var sawSSH, sawRsync bool
	for _, s := range cfg.Syncs {
		switch s.Mode {
		case ModeRsyncSSH:
			sawSSH = true
			if s.Target != "backup@10.0.0.9" {
				t.Errorf("rsyncssh target = %q", s.Target)
			}
			if s.TargetDir != "/backup/data" {
				t.Errorf("rsyncssh target_dir = %q", s.TargetDir)
			}
			if s.SSH.Port != 22 || s.SSH.IdentityFile == "" {
				t.Errorf("ssh settings not read: %+v", s.SSH)
			}
			if s.SSH.Options["BatchMode"] != "yes" {
				t.Errorf("ssh.options not read: %+v", s.SSH.Options)
			}
		case ModeRsync:
			sawRsync = true
		}
	}
	if !sawSSH || !sawRsync {
		t.Fatalf("uncommented example should hold rsyncssh and rsync syncs; got ssh=%v rsync=%v", sawSSH, sawRsync)
	}
}

func parseExample(t *testing.T) Config {
	t.Helper()
	path := filepath.Join("..", "..", "examples", "gsyncd.example.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	cfg, err := Parse(string(data))
	if err != nil {
		t.Fatalf("%s does not parse: %v", path, err)
	}
	return cfg
}

// uncommentSyncBlocks returns the text of every run of comment lines that
// starts with a commented section header, with one leading '#' and one space
// removed per line. Prose comment blocks are never entered because they do not
// open with a header.
func uncommentSyncBlocks(src string) string {
	var out []string
	in := false
	for _, line := range strings.Split(src, "\n") {
		trim := strings.TrimLeft(line, " \t")
		switch {
		case strings.HasPrefix(trim, "# [["):
			in = true
		case strings.HasPrefix(trim, "# ["):
			// [sync.ssh] belongs to the block that precedes it.
			if !in {
				continue
			}
		case trim == "":
			// A blank line ends the block.
			in = false
			continue
		case !strings.HasPrefix(trim, "#"):
			in = false
			continue
		}
		if !in {
			continue
		}
		body := strings.TrimPrefix(trim, "#")
		out = append(out, strings.TrimPrefix(body, " "))
	}
	return strings.Join(out, "\n")
}
