//go:build linux

package guard

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
)

// ioPrioSet is __NR_ioprio_set, which the syscall package does not wrap.
//
// The number is per architecture, so it is looked up at run time instead of
// being pinned by a build tag: a wrong constant would land on some other
// syscall, which is far worse than the honest error an unknown architecture
// gets.
var ioPrioSet = map[string]uintptr{
	"amd64":   251,
	"386":     289,
	"arm64":   30,
	"arm":     314,
	"riscv64": 30,
	"ppc64":   273,
	"ppc64le": 273,
	"s390x":   282,
}

// ioPrioWhoProcess is IOPRIO_WHO_PROCESS: apply to the calling process.
const ioPrioWhoProcess = 1

// applyIOPrio sets the io scheduling class of the calling thread, and of every
// other thread in the process.
//
// ioprio_set, like setpriority, is a per-thread attribute on Linux. Setting it
// once leaves the worker goroutines in the default class, which is exactly the
// ones doing the copying.
func applyIOPrio(class, level int) error {
	nr, ok := ioPrioSet[runtime.GOARCH]
	if !ok {
		return fmt.Errorf("io priority is not implemented for linux/%s", runtime.GOARCH)
	}
	prio := uintptr(class<<13 | level)
	if err := setIOPrioOne(nr, 0, prio); err != nil {
		return err
	}
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil
	}
	for _, e := range ents {
		tid, cerr := strconv.Atoi(e.Name())
		if cerr != nil {
			continue
		}
		// ESRCH: the thread exited between the readdir and the call. Not an
		// error - the same race exists in applyNice.
		_ = setIOPrioOne(nr, tid, prio)
	}
	return nil
}

func setIOPrioOne(nr uintptr, tid int, prio uintptr) error {
	_, _, e := syscall.Syscall(nr, ioPrioWhoProcess, uintptr(tid), prio)
	if e != 0 {
		if e == syscall.ESRCH {
			return nil
		}
		return e
	}
	return nil
}

// currentNice reports the nice value of the calling thread, which on Linux is
// what setpriority changes. It exists for the test that proves Apply actually
// moved the priority rather than merely returning nil.
func currentNice() (int, error) {
	return syscall.Getpriority(syscall.PRIO_PROCESS, 0)
}

// applyNice sets the nice value of every thread that currently exists in the
// process.
//
// setpriority on Linux acts on one thread, and Go's scheduler moves goroutines
// between threads at will, so setting it once on the calling thread leaves the
// copy workers - the goroutines that actually saturate the disk - at the
// default priority. Sweeping /proc/self/task also covers threads the runtime
// created before we got here, and every thread spawned afterwards inherits the
// niceness of whichever thread spawned it, which is now a niced one.
//
// A thread that exits mid-sweep is not an error; anything else is reported.
func applyNice(nice int) error {
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		// No procfs (a hardened container, say). Fall back to the calling
		// thread: less than we wanted, still better than nothing.
		return syscall.Setpriority(syscall.PRIO_PROCESS, 0, nice)
	}
	set := 0
	for _, e := range ents {
		tid, cerr := strconv.Atoi(e.Name())
		if cerr != nil {
			continue
		}
		if err := syscall.Setpriority(syscall.PRIO_PROCESS, tid, nice); err != nil {
			if err == syscall.ESRCH {
				continue
			}
			return err
		}
		set++
	}
	if set == 0 {
		return fmt.Errorf("no thread could be reniced")
	}
	return nil
}
