package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

// procMemory reports the resident set size and the live Go heap, in KiB.
//
// RSS is read from /proc/self/statm rather than from the runtime because the
// question an operator asks on a huge tree is "how much of this machine is
// this daemon holding", and the runtime only knows its own heap. The second
// field of statm is the resident page count. On a platform without /proc the
// value is simply absent, which the status document tolerates.
func procMemory() (rssKB, heapKB int64) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// HeapAlloc, not Sys: Sys includes address space the runtime reserved but
	// never touched, which would overstate the footprint by an order of
	// magnitude on a machine with a large memory_limit.
	heapKB = int64(ms.HeapAlloc >> 10)

	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, heapKB
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, heapKB
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, heapKB
	}
	return pages * int64(os.Getpagesize()) >> 10, heapKB
}
