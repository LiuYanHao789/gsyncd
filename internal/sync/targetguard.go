package sync

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TargetMarkerName is the file that proves the target filesystem was mounted
// and initialised by the operator. It lives inside the target so that it
// disappears with the mount: that is the whole point. It is excluded from
// pruning and from the rsync argument list.
const TargetMarkerName = ".gsyncd-target"

const targetMarkerMagic = "gsyncd-target-v1\n"

// CheckTarget refuses to write unless the target already exists and is either
// on a non-root mount or carries the marker written by InitTarget.
//
// The failure this prevents is not hypothetical: an unmounted backup disk
// leaves the mount point as an ordinary directory on the root filesystem, so
// creating the target would happily fill "/" with a mirror. A marker on the
// target filesystem cannot survive the unmount.
func CheckTarget(target string) error {
	abs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("target %s is not usable (is the filesystem mounted?): %w", abs, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("target %s exists but is not a directory", abs)
	}
	// Resolve symlinks before deciding what filesystem this is. The lexical
	// mountinfo check accepted /dev/shm/link -> /srv (which writes to the root
	// filesystem) and rejected /backup -> /mnt/disk (which is correctly
	// mounted).
	real, rerr := filepath.EvalSymlinks(abs)
	if rerr != nil {
		return fmt.Errorf("target %s cannot be resolved: %w", abs, rerr)
	}
	realSt, err := os.Stat(real)
	if err != nil || !realSt.IsDir() {
		return fmt.Errorf("target %s resolves to a non-directory", abs)
	}
	if targetMarkerOK(abs, real) {
		return nil
	}
	// A target on a different filesystem than "/" is accepted even without a
	// marker: that is the mounted backup disk. Same-device targets (bind
	// mounts of the root filesystem, plain directories) need the marker.
	rootSt, err := os.Stat("/")
	if err != nil {
		return fmt.Errorf("cannot determine the root filesystem: %w", err)
	}
	if differentFilesystem(rootSt, realSt) {
		return nil
	}
	return fmt.Errorf("target %s is neither a mount point nor marked with %s; refusing to write "+
		"(mount the target filesystem and run gsyncd with -init-target)", abs, TargetMarkerName)
}

// InitTarget writes the marker that CheckTarget accepts. It requires the target
// directory to exist: creating it would defeat the check.
func InitTarget(target string) error {
	abs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("target %s is not usable (is the filesystem mounted?): %w", abs, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("target %s exists but is not a directory", abs)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return fmt.Errorf("target %s cannot be resolved: %w", abs, err)
	}
	// The marker binds to the physical target path. Use a private temp file and
	// rename so a planted .tmp symlink cannot redirect the write.
	tmp, err := os.CreateTemp(real, TargetMarkerName+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	body := targetMarkerMagic + real + "\n"
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	// CreateTemp uses 0600; the marker must be readable by a daemon running as
	// a different user than the one that initialised the target.
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(real, TargetMarkerName)); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// targetMarkerOK accepts a marker written for either the configured path or
// the resolved path, so existing deployments keep working after the switch to
// physical paths.
func targetMarkerOK(paths ...string) bool {
	for _, p := range paths {
		data, err := os.ReadFile(filepath.Join(p, TargetMarkerName))
		if err != nil {
			continue
		}
		for _, want := range paths {
			if string(data) == targetMarkerMagic+want+"\n" {
				return true
			}
		}
	}
	return false
}

// onNonRootMount reports whether path sits on a mount point other than "/".
// A target on the root filesystem needs a marker; a target below a real mount
// (a backup disk, an NFS export) is accepted directly.
func onNonRootMount(path string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()
	abs := filepath.Clean(path)
	best := "/"
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}
		mp := unescapeMount(fields[4])
		if !isPathPrefix(mp, abs) {
			continue
		}
		if len(mp) > len(best) {
			best = mp
		}
	}
	if err := sc.Err(); err != nil {
		return false, err
	}
	return best != "/", nil
}

func isPathPrefix(root, p string) bool {
	if root == "/" {
		return strings.HasPrefix(p, "/")
	}
	if p == root {
		return true
	}
	return strings.HasPrefix(p, root+"/")
}

func unescapeMount(s string) string {
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return filepath.Clean(r.Replace(s))
}
