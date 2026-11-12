package main

// Times only the full-tree copy (Direct.Startup) at a given worker count, so
// the concurrency question can be answered without the watcher or the
// aggregator in the way.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	syncpkg "gsyncd/internal/sync"
)

// resolveDeepest resolves symlinks in the longest existing prefix of p and
// re-appends the missing components.
func resolveDeepest(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	var suffix []string
	cur := p
	for {
		parent := filepath.Dir(cur)
		if parent == cur {
			return p
		}
		suffix = append([]string{filepath.Base(cur)}, suffix...)
		cur = parent
		if r, err := filepath.EvalSymlinks(cur); err == nil {
			parts := append([]string{r}, suffix...)
			return filepath.Join(parts...)
		}
	}
}

// relWithin reports whether p is root or below it.
func relWithin(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func main() {
	src := flag.String("src", "", "source")
	dst := flag.String("dst", "", "target")
	workers := flag.Int("workers", 8, "copy workers")
	flag.Parse()
	absSrc, err := filepath.Abs(*src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "copybench: -src: %v\n", err)
		os.Exit(2)
	}
	absDst, err := filepath.Abs(*dst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "copybench: -dst: %v\n", err)
		os.Exit(2)
	}
	absSrc = resolveDeepest(absSrc)
	// Validate the source before RemoveAll(dst): a typo must not delete the
	// target and then fail.
	if st, e := os.Lstat(absSrc); e != nil || !st.IsDir() {
		fmt.Fprintf(os.Stderr, "copybench: -src %s is not a usable directory: %v\n", absSrc, e)
		os.Exit(2)
	}
	// Resolve the deepest existing ancestor: -dst may not exist yet, and a
	// symlinked ancestor must not hide an overlap with -src.
	absDst = resolveDeepest(absDst)
	vol := filepath.VolumeName(absDst)
	root := string(filepath.Separator)
	if vol != "" {
		root = vol + string(filepath.Separator)
	}
	if *src == "" || *dst == "" || absDst == root || absDst == vol ||
		absSrc == absDst || relWithin(absDst, absSrc) || relWithin(absSrc, absDst) {
		fmt.Fprintln(os.Stderr, "copybench: -src and -dst are required; -dst must be a non-root path that does not contain or sit inside -src")
		os.Exit(2)
	}
	os.RemoveAll(absDst)
	d := syncpkg.NewDirect(absSrc, absDst, false, *workers, func(string, string, ...any) {})
	t0 := time.Now()
	if err := d.Startup(context.Background()); err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
	el := time.Since(t0)
	st := d.Stats()
	fmt.Printf("workers=%d src=%s elapsed=%v files=%d deletes=%d errors=%d\n",
		*workers, *src, el.Round(time.Millisecond), st.Files, st.Deletes, st.Errors)
}
