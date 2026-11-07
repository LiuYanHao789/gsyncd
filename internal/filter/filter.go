// Package filter implements rsync-style exclude patterns.
//
// The original lsyncd shipped excludes to rsync and additionally kept a Lua
// table of them; on 100M file trees the per-event string matching dominated the
// CPU profile. Here patterns are compiled once and every event is tested
// against them with a single pass, so the cost is bounded and predictable.
package filter

import (
	"strings"
)

// Pattern is one compiled exclude expression.
type Pattern struct {
	raw      string
	anchored bool
	dirOnly  bool
	segments []string
	negate   bool
	// wildcard records a `**` segment, which makes the pattern length
	// variable and rules out the window scan in matchParts.
	wildcard bool
	// single marks an unanchored one-segment pattern such as `*.tmp` or
	// `node_modules`. Those can be tested by walking separators instead of
	// splitting the path, which is the difference between one allocation per
	// event and none.
	single bool
}

// Filter is a compiled pattern list.
//
// An earlier version memoised decisions in a map keyed by full path. Measured
// on unique paths it was a net loss - 1214 ns/op cached against 872 ns/op
// uncached, because the cache never hit - and the cap of 200k entries retained
// 13 MiB of strings for decisions that were never asked again. A tree large
// enough to matter is a tree where nearly every path is unique, so the cache
// was removed rather than tuned. The pattern list itself is compiled once and
// shared read-only, so Match is safe to call from the copy workers.
type Filter struct {
	pats []Pattern
}

// New compiles a pattern list.
func New(patterns []string) *Filter {
	f := &Filter{}
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" || strings.HasPrefix(p, "#") {
			continue
		}
		pat := Pattern{raw: p}
		if strings.HasPrefix(p, "!") {
			pat.negate = true
			p = p[1:]
		}
		if strings.HasSuffix(p, "/") {
			pat.dirOnly = true
			p = strings.TrimSuffix(p, "/")
		}
		if strings.HasPrefix(p, "/") {
			pat.anchored = true
			p = strings.TrimPrefix(p, "/")
		}
		pat.segments = strings.Split(p, "/")
		for _, s := range pat.segments {
			if s == "**" {
				pat.wildcard = true
				break
			}
		}
		pat.single = !pat.anchored && !pat.wildcard && len(pat.segments) == 1
		f.pats = append(f.pats, pat)
	}
	return f
}

// Empty reports whether the filter can never match.
func (f *Filter) Empty() bool { return f == nil || len(f.pats) == 0 }

// Match reports whether rel (a slash separated path relative to the sync root)
// is excluded. dir reports whether the subject is a directory.
func (f *Filter) Match(rel string, dir bool) bool {
	if f.Empty() {
		return false
	}
	// Patterns are always slash separated. Normalising here rather than at
	// every call site keeps a Windows path from quietly matching nothing.
	if strings.IndexByte(rel, '\\') >= 0 {
		rel = strings.ReplaceAll(rel, "\\", "/")
	}
	// Split once and share the result across patterns. Splitting inside the
	// pattern loop allocated a slice per pattern per call, which on an 8-rule
	// filter was eight slices for one decision - and the decision is made for
	// every entry of every walk, plus every event.
	//
	// The split is also deferred: one-segment patterns are tested by walking
	// separators, so a filter made only of `*.tmp`-shaped rules never
	// allocates at all.
	res := false
	var parts []string
	for i := range f.pats {
		p := &f.pats[i]
		var hit bool
		if p.single {
			hit = p.matchSingle(rel, dir)
		} else {
			if parts == nil {
				parts = strings.Split(rel, "/")
			}
			hit = p.matchParts(parts, dir)
		}
		if hit {
			res = !p.negate
		}
	}
	return res
}

// matchSingle matches a one-segment unanchored pattern without splitting the
// path. This is the shape of the rules people actually write (`*.tmp`,
// `*.swp`, `node_modules`), and it is decided for every entry of every walk
// and every event, so it must not allocate.
func (p *Pattern) matchSingle(rel string, dir bool) bool {
	limit := len(rel)
	if p.dirOnly && !dir {
		// A directory pattern must not match the subject itself when the
		// subject is a file: `tmp/` excludes `x/tmp/file` but not a plain
		// file called `tmp`.
		i := strings.LastIndexByte(rel, '/')
		if i < 0 {
			return false
		}
		limit = i
	}
	if limit == 0 {
		return false
	}
	start := 0
	for i := 0; i <= limit; i++ {
		if i == limit || rel[i] == '/' {
			if i > start && matchOne(p.segments[0], rel[start:i]) {
				return true
			}
			start = i + 1
		}
	}
	return false
}

// matchParts tests one pattern against the already split path.
//
// A pattern covers everything below a match, not just the entry itself: rsync
// never descends into an excluded directory, so `/cache/` means "and all of its
// contents", and the original lsyncd got the same effect by appending '/' to
// the Lua pattern. Matching only the exact path let a file created later inside
// an excluded directory through, which is the hole that made an excluded cache
// directory reappear on the target.
//
// A directory pattern (`tmp/`) may only match a proper ancestor when the
// subject is a file, so it excludes `x/tmp/file` but not a plain file `tmp`.
func (p Pattern) matchParts(parts []string, dir bool) bool {
	limit := len(parts)
	if p.dirOnly && !dir {
		limit--
	}
	if limit <= 0 {
		return false
	}
	if p.wildcard {
		// A `**` makes the pattern length variable, so the only correct test is
		// every prefix. Rare enough not to matter.
		for k := 1; k <= limit; k++ {
			if p.matchSegs(parts[:k]) {
				return true
			}
		}
		return false
	}
	n := len(p.segments)
	if n == 0 {
		return false
	}
	if p.anchored {
		// An anchored pattern always starts at the root, so it covers the path
		// as soon as its own length fits.
		return limit >= n && matchSegments(p.segments, parts[:n])
	}
	// Unanchored: the pattern may sit anywhere as long as it ends at or before
	// the subject, i.e. it is a contiguous window of exactly n segments.
	for i := 0; i+n <= limit; i++ {
		if matchSegments(p.segments, parts[i:i+n]) {
			return true
		}
	}
	return false
}

// matchSegs reports whether the whole of parts matches the pattern.
func (p Pattern) matchSegs(parts []string) bool {
	if p.anchored {
		return matchSegments(p.segments, parts)
	}
	for i := 0; i < len(parts); i++ {
		if matchSegments(p.segments, parts[i:]) {
			return true
		}
	}
	return false
}

func matchSegments(pat, path []string) bool {
	if len(pat) == 0 {
		return len(path) == 0
	}
	if pat[0] == "**" {
		if matchSegments(pat[1:], path) {
			return true
		}
		if len(path) > 0 {
			return matchSegments(pat, path[1:])
		}
		return false
	}
	if len(path) == 0 {
		return false
	}
	if !matchOne(pat[0], path[0]) {
		return false
	}
	return matchSegments(pat[1:], path[1:])
}

// matchOne is a shell glob matcher for a single path segment.
//
// Supports `*`, `?` and `[a-z]` / `[!abc]` character classes, which is what
// rsync accepts. The original translated patterns with Lua's own escaping and
// ended up making `[` and `]` literal, so `tmp[0-9]` matched nothing on a
// running lsyncd while the same pattern worked when handed to rsync.
func matchOne(pat, s string) bool {
	// Iterative backtracking keeps this allocation free.
	var pi, si, star, match int
	star = -1
	for si < len(s) {
		if pi < len(pat) {
			switch pat[pi] {
			case '?':
				pi++
				si++
				continue
			case '*':
				star = pi
				match = si
				pi++
				continue
			case '[':
				if n, ok := matchClass(pat[pi:], s[si]); ok {
					pi += n
					si++
					continue
				}
			default:
				if pat[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}
		if star >= 0 {
			pi = star + 1
			match++
			si = match
			continue
		}
		return false
	}
	for pi < len(pat) && pat[pi] == '*' {
		pi++
	}
	return pi == len(pat)
}

// matchClass tests s against the character class starting at pat[0] == '['.
// It returns the class width in bytes and whether s matched. An unterminated
// class is treated as a literal '[' so a malformed pattern never silently
// matches everything.
func matchClass(pat string, s byte) (int, bool) {
	i := 1
	neg := false
	if i < len(pat) && (pat[i] == '!' || pat[i] == '^') {
		neg = true
		i++
	}
	hit := false
	first := true
	for i < len(pat) {
		if pat[i] == ']' && !first {
			if neg {
				hit = !hit
			}
			return i + 1, hit
		}
		first = false
		lo := pat[i]
		if lo == '\\' && i+1 < len(pat) {
			i++
			lo = pat[i]
		}
		if i+2 < len(pat) && pat[i+1] == '-' && pat[i+2] != ']' {
			hi := pat[i+2]
			if s >= lo && s <= hi {
				hit = true
			}
			i += 3
			continue
		}
		if s == lo {
			hit = true
		}
		i++
	}
	return 1, s == '['
}
