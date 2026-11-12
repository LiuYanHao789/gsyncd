package filter

import (
	"strings"
	"testing"
)

func TestFilterBasics(t *testing.T) {
	cases := []struct {
		pats []string
		rel  string
		dir  bool
		want bool
	}{
		{[]string{"*.log"}, "a/b/c.log", false, true},
		{[]string{"*.log"}, "a/b/c.txt", false, false},
		{[]string{"/build"}, "build", true, true},
		{[]string{"/build"}, "src/build", true, false},
		{[]string{"tmp/"}, "x/tmp", true, true},
		{[]string{"tmp/"}, "x/tmp", false, false},
		{[]string{".git", "*.o"}, "x/.git", true, true},
		{[]string{"**/node_modules/**"}, "a/node_modules/b/c.js", false, true},
		{[]string{"a/*/c"}, "a/b/c", false, true},
		{[]string{"a/*/c"}, "a/b/d/c", false, false},
		{[]string{"*.tmp", "!keep.tmp"}, "x/keep.tmp", false, false},
		{[]string{"*.tmp", "!keep.tmp"}, "x/other.tmp", false, true},

		// A directory pattern covers everything inside that directory,
		// otherwise a file created later inside it slips through.
		{[]string{"cache/"}, "cache/blob.bin", false, true},
		{[]string{"cache/"}, "a/cache/b/c.bin", false, true},
		{[]string{"/cache/"}, "cache/b/c.bin", false, true},
		{[]string{"/cache/"}, "a/cache/b", false, false},
		{[]string{"tmp/"}, "x/tmp/deep/file", false, true},
		// ...but a directory-only pattern still must not match a file whose
		// name merely looks like the directory.
		{[]string{"tmp/"}, "x/tmp", false, false},
		{[]string{"tmp/"}, "x/tmpfile", false, false},

		// Windows paths arrive with backslashes; patterns are always slash
		// separated, so the separator has to be normalised on the way in.
		{[]string{"*.tmp"}, `x\y\a.tmp`, false, true},
		{[]string{"/cache/"}, `cache\blob.bin`, false, true},

		// Any pattern that matches a directory covers its contents, not just
		// the entry itself. This is what rsync does by never descending.
		{[]string{"*.log"}, "x.log/inside.txt", false, true},
		{[]string{"a/*/c"}, "a/b/c/d.txt", false, true},
		{[]string{"a/*/c"}, "a/b/d/c", false, false},

		// rsync-style character classes. The Lua version escaped '[' and ']'
		// into literals, so this pattern excluded nothing at runtime while
		// still being forwarded to rsync verbatim.
		{[]string{"tmp[0-9]"}, "x/tmp5", true, true},
		{[]string{"tmp[0-9]"}, "x/tmpZ", true, false},
		{[]string{"tmp[!0-9]"}, "x/tmpZ", true, true},
		{[]string{"tmp[!0-9]"}, "x/tmp5", true, false},
		{[]string{"a[b-d]x"}, "abx", true, true},
		{[]string{"a[b-d]x"}, "aex", true, false},
	}
	for _, c := range cases {
		f := New(c.pats)
		if got := f.Match(c.rel, c.dir); got != c.want {
			t.Errorf("pats=%v rel=%q dir=%v: got %v want %v", c.pats, c.rel, c.dir, got, c.want)
		}
	}
}

func TestMatchOne(t *testing.T) {
	cases := [][3]any{
		{"abc", "abc", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbb", false},
		{"a?c", "abc", true},
		{"*", "anything", true},
	}
	for _, c := range cases {
		if got := matchOne(c[0].(string), c[1].(string)); got != c[2].(bool) {
			t.Errorf("matchOne(%q,%q)=%v", c[0], c[1], got)
		}
	}
}
func BenchmarkMatchMiss(b *testing.B) {
	f := New([]string{"*.log", "*.tmp", "/cache/", "**/node_modules/**", "build/"})
	rels := []string{
		"src/internal/engine/engine.go",
		"src/internal/sync/direct.go",
		"docs/guide/getting-started.md",
		"src/app/handlers/users/list.go",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match(rels[i%len(rels)]+string(rune('0'+i%10)), false)
	}
}

// BenchmarkMatchRepeatedPath is the one case a per-path cache would have won:
// the same path asked over and over. It is kept to record why that did not
// matter - a real tree asks about nearly every path exactly once.
func BenchmarkMatchRepeatedPath(b *testing.B) {
	f := New([]string{"*.log", "*.tmp", "/cache/"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match("src/internal/engine/engine.go", false)
	}
}

// TestPatternCoversDescendants pins the rule that made an excluded directory
// reappear on the target: a pattern matching a directory must also cover
// everything below it, because rsync never descends into it.
func TestPatternCoversDescendants(t *testing.T) {
	f := New([]string{"/cache/"})
	for _, rel := range []string{"cache", "cache/a", "cache/a/b/c.bin"} {
		if !f.Match(rel, rel == "cache") {
			t.Errorf("Match(%q) = false, want true", rel)
		}
	}
	if f.Match("other/cache", true) {
		t.Error("anchored /cache/ must not match a nested cache")
	}
}

// TestCharacterClass pins rsync-compatible bracket expressions. The original
// escaped '[' and ']' into literals, so such a rule excluded nothing at
// runtime while still being forwarded to rsync unchanged.
func TestCharacterClass(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[a-c]", "b", true},
		{"[a-c]", "z", false},
		{"[!a-c]", "z", true},
		{"[!a-c]", "a", false},
		{"[]]", "]", true},
		// An unterminated class falls back to a literal '[', so a malformed
		// pattern can never silently match everything.
		{"[a-", "[a-", true},
		{"[a-", "a-", false},
	}
	for _, c := range cases {
		if got := matchOne(c.pat, c.s); got != c.want {
			t.Errorf("matchOne(%q, %q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
}

// The single-segment fast path must decide exactly what the general path
// decides. A fast path that changes which files are excluded is a correctness
// bug that only shows up as "a file that should have been ignored got synced".
func TestSingleFastPathAgreesWithGeneral(t *testing.T) {
	pats := []string{"*.tmp", "*.swp", "node_modules", "node_modules/", "*.log", "tmp/", "a*b", "?x", "[0-9]z", "sub/*.o"}
	paths := []string{
		"a.tmp", "tmp", "tmp/x", "x/tmp", "x/tmp/y", "x/y.tmp", "node_modules", "node_modules/a/b",
		"x/node_modules", "x/node_modules/a", "a.log", "x/y/a.log", "ab", "aXb", "ax", "5z", "sub/a.o",
		"sub/deep/a.o", "b.tmp/c", "TMP", "x/tmp/", "", "/", "a//b.tmp",
	}
	for _, pat := range pats {
		f := New([]string{pat})
		p := &f.pats[0]
		if !p.single {
			continue
		}
		for _, rel := range paths {
			for _, dir := range []bool{false, true} {
				fast := p.matchSingle(rel, dir)
				slow := p.matchParts(strings.Split(rel, "/"), dir)
				if fast != slow {
					t.Errorf("pattern %q path %q dir=%v: fast=%v general=%v", pat, rel, dir, fast, slow)
				}
			}
		}
	}
}

// And the whole filter must keep the semantics the rest of the program relies
// on, including the rule that a directory pattern covers its contents.
func TestFastPathKeepsDescendantRule(t *testing.T) {
	f := New([]string{"node_modules/", "*.tmp"})
	for _, rel := range []string{"node_modules", "a/node_modules", "a/node_modules/b/c.js"} {
		if !f.Match(rel, rel == "node_modules" || rel == "a/node_modules") {
			t.Errorf("Match(%q) = false, want true", rel)
		}
	}
	if !f.Match("a/b/x.tmp", false) {
		t.Error("*.tmp must match at any depth")
	}
	if f.Match("node_modulesx", true) {
		t.Error("node_modules/ must not match a sibling with the same prefix")
	}
}

func TestTripleStarMatchesDirectoryAndContents(t *testing.T) {
	f := New([]string{"cache/***"})
	if !f.Match("cache", true) || !f.Match("cache/x", false) || !f.Match("a/cache/x", false) {
		t.Fatal("cache/*** must match the directory itself and everything below it")
	}
	g := New([]string{"cache/**"})
	if g.Match("cache", true) {
		t.Fatal("cache/** must not match the directory itself (rsync semantics)")
	}
	if !g.Match("cache/x", false) {
		t.Fatal("cache/** must match contents")
	}
}

func TestEscapedLeadingBangIsLiteral(t *testing.T) {
	f := New([]string{`\!keep`})
	if !f.Match("!keep", false) {
		t.Fatal(`\!keep must exclude the literal name "!keep"`)
	}
	if f.Match("anything", false) {
		t.Fatal("escaped pattern must not negate")
	}
}

func TestLeadingDoubleStarMatchesZeroSegments(t *testing.T) {
	f := New([]string{"**/.git/**"})
	if !f.Match(".git/config", false) {
		t.Fatal("**/.git/** must match a root-level .git subtree")
	}
	g := New([]string{"**/node_modules/***"})
	if !g.Match("node_modules", true) || !g.Match("node_modules/a", false) {
		t.Fatal("**/node_modules/*** must match a root-level node_modules tree")
	}
}

func TestTripleStarDoesNotMatchRegularFileWithDirName(t *testing.T) {
	f := New([]string{"cache/***"})
	if f.Match("cache", false) {
		t.Fatal("dir/*** is directory-only for the directory itself")
	}
}
