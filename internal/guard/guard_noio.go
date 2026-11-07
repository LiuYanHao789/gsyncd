//go:build unix && !linux

package guard

import (
	"fmt"
	"runtime"
)

// applyIOPrio reports that io priorities are a Linux concept.
//
// Darwin has a different mechanism (setiopolicy_np) with different semantics,
// and pretending the Linux names map onto it would be a lie in the log.
func applyIOPrio(class, level int) error {
	return fmt.Errorf("io priority is not implemented on %s", runtime.GOOS)
}
