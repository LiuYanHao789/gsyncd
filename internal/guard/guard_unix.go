//go:build unix

package guard

import "fmt"

// Apply lowers this process's scheduling priority.
//
// Called once, early, before any backend starts, so that everything the process
// later does - copy goroutines, watcher threads, rsync children - is scheduled
// behind the services the host actually exists to serve.
//
// A setpriority call on Linux affects one thread, not the process, and Go moves
// goroutines between threads freely - so niceness is applied to every thread in
// the process rather than to whichever one happens to run this function. See
// applyNice in guard_linux.go for why that sweep matters.
func Apply(p Policy) error {
	if !p.Active() {
		return nil
	}
	if p.Nice != 0 {
		if err := applyNice(p.Nice); err != nil {
			return fmt.Errorf("cannot set nice %d: %w", p.Nice, err)
		}
	}
	if p.IOPrio != "" {
		class, level, err := parseIOPrio(p.IOPrio)
		if err != nil {
			return err
		}
		if err := applyIOPrio(class, level); err != nil {
			return err
		}
	}
	return nil
}
