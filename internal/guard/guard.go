// Package guard lowers the priority of the process so that a large initial
// copy cannot starve the services sharing the machine.
//
// The problem it solves is interference, not throughput. gsyncd is happy to
// saturate every core and the disk for hours while it seeds a mirror, and on a
// host that also runs java, mysql or redis that means those services see their
// own latency multiplied. Everything here is best effort: if a kernel or a
// container forbids the call, gsyncd logs it and carries on, because a mirror
// that runs at normal priority is still a working mirror.
package guard

import (
	"fmt"
	"strconv"
	"strings"
)

// Policy is the resource posture requested by the configuration.
type Policy struct {
	// Nice is the CPU scheduling niceness, -20 (most favoured) to 19 (least).
	// Zero is the default and means "do not change anything": a host that is
	// nothing but a mirror server should not be slowed down for nothing.
	Nice int
	// IOPrio is the io scheduling class: "" (leave alone), "idle",
	// "best-effort" or "realtime", optionally with a level as in
	// "best-effort:7". Idle is the one that matters here - on a busy disk it
	// makes the kernel hand the bandwidth to everyone else first.
	IOPrio string
}

// Active reports whether the policy asks for any change at all.
func (p Policy) Active() bool { return p.Nice != 0 || p.IOPrio != "" }

// parseIOPrio turns "idle", "best-effort:7" or "realtime:1" into the class and
// level ioprio_set expects.
//
// Classes are fixed by the kernel: 1 realtime, 2 best-effort, 3 idle. The level
// is 0-7, where a higher number is a lower priority inside the class; the idle
// class ignores it.
func parseIOPrio(s string) (class, level int, err error) {
	name := strings.ToLower(strings.TrimSpace(s))
	if i := strings.IndexByte(name, ':'); i >= 0 {
		lv, cerr := strconv.Atoi(strings.TrimSpace(name[i+1:]))
		if cerr != nil || lv < 0 || lv > 7 {
			return 0, 0, fmt.Errorf("io priority %q: level must be 0-7", s)
		}
		level = lv
		name = strings.TrimSpace(name[:i])
	}
	switch name {
	case "realtime":
		return 1, level, nil
	case "best-effort", "best_effort", "besteffort":
		return 2, level, nil
	case "idle":
		return 3, 0, nil
	}
	return 0, 0, fmt.Errorf("io priority %q: expected idle, best-effort or realtime", s)
}

// Describe renders the policy for a log line.
func (p Policy) Describe() string {
	switch {
	case p.Nice != 0 && p.IOPrio != "":
		return "nice " + strconv.Itoa(p.Nice) + ", io " + p.IOPrio
	case p.Nice != 0:
		return "nice " + strconv.Itoa(p.Nice)
	default:
		return "io " + p.IOPrio
	}
}
