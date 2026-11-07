//go:build windows

package guard

import "fmt"

// Apply reports that the policy cannot be honoured on Windows.
//
// It returns an error rather than silently doing nothing because an operator
// who wrote nice = 10 deserves to know it was ignored. The caller logs it and
// carries on: refusing to mirror a filesystem over an unhonoured priority hint
// would be the worse trade.
func Apply(p Policy) error {
	if !p.Active() {
		return nil
	}
	return fmt.Errorf("nice/io priority is not supported on windows")
}
