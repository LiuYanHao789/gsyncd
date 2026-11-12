package sync

import "sync/atomic"

// tmpSeq makes staging names unique across platforms (fileio_unix and the
// Windows rename fallback both use it).
var tmpSeq uint64

func nextTmpSeq() uint64 { return atomic.AddUint64(&tmpSeq, 1) }
