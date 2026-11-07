package main

// Times only the full-tree copy (Direct.Startup) at a given worker count, so
// the concurrency question can be answered without the watcher or the
// aggregator in the way.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	syncpkg "gsyncd/internal/sync"
)

func main() {
	src := flag.String("src", "", "source")
	dst := flag.String("dst", "", "target")
	workers := flag.Int("workers", 8, "copy workers")
	flag.Parse()
	os.RemoveAll(*dst)
	d := syncpkg.NewDirect(*src, *dst, false, *workers, func(string, string, ...any) {})
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
