//go:build linux

// Command probemark measures what watching a tree actually costs the kernel.
//
// It installs inotify watches on every directory below -root and holds them
// until it is killed. Run it while sampling /proc/meminfo's SUnreclaim (or
// /proc/slabinfo) to get the per-directory figure that decides whether a tree
// with tens of millions of directories can be watched at all. gsyncd's own
// index is measured separately by TestMemoryPerDirectory; this is the other
// half of the budget, and the larger half.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gsyncd/internal/watcher"
)

func main() {
	root := flag.String("root", "", "directory tree to watch")
	hold := flag.Duration("hold", 10*time.Minute, "how long to keep the watches")
	flag.Parse()
	if *root == "" {
		fmt.Fprintln(os.Stderr, "probemark: -root is required")
		os.Exit(2)
	}

	logf := func(format string, args ...any) { fmt.Printf(format+"\n", args...) }
	be, err := watcher.NewInotify(logf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "probemark: %v\n", err)
		os.Exit(1)
	}
	tree := watcher.New(be, watcher.Options{Workers: 8, Logf: logf})
	start := time.Now()
	if err := tree.AddRoot(*root); err != nil {
		fmt.Fprintf(os.Stderr, "probemark: %v\n", err)
		os.Exit(1)
	}
	n := tree.WatchDirCount()
	el := time.Since(start)
	fmt.Printf("watching %d directories in %s (%.0f dirs/s), unwatched=%d\n",
		n, el.Round(time.Millisecond), float64(n)/el.Seconds(), tree.Unwatched())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-time.After(*hold):
	}
	fmt.Println("releasing watches")
	_ = tree.Close()
	_ = be.Close()
}
