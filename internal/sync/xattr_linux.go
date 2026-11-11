//go:build linux

package sync

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
)

// copyXattrs mirrors every extended attribute from src to dst.
//
// POSIX ACLs are stored in the system.posix_acl_access and
// system.posix_acl_default attributes on Linux, so this covers ACLs as well.
// A name the target refuses (a security.* label it cannot set, an attribute
// the filesystem does not support) is skipped and the first failure is
// returned; the caller logs it once and keeps the copy.
func copyXattrs(src, dst string) error {
	names, err := listXattrNames(src)
	if err != nil {
		if isXattrUnsupported(err) {
			return nil
		}
		return err
	}
	var first error
	srcSet := make(map[string]bool, len(names))
	for _, name := range names {
		srcSet[name] = true
		val, err := getXattr(src, name)
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		// Rewriting an identical attribute costs a syscall and can trigger a
		// ctime change; skip it.
		if cur, err := getXattr(dst, name); err == nil && bytes.Equal(cur, val) {
			continue
		}
		if err := syscall.Setxattr(dst, name, val, 0); err != nil {
			if first == nil {
				first = err
			}
		}
	}
	// An attribute that was removed at the source has to disappear at the
	// target too, or revoking a POSIX ACL never propagates. Only the
	// namespaces this code owns are touched: user.* and the ACL attributes.
	// security.* and trusted.* may be set by the target host itself and are
	// left alone.
	dstNames, err := listXattrNames(dst)
	if err != nil {
		if first == nil && !isXattrUnsupported(err) {
			first = err
		}
		return first
	}
	for _, name := range dstNames {
		if srcSet[name] {
			continue
		}
		if !strings.HasPrefix(name, "user.") && !strings.HasPrefix(name, "system.posix_acl_") {
			continue
		}
		if err := syscall.Removexattr(dst, name); err != nil {
			if first == nil {
				first = err
			}
		}
	}
	return first
}

func listXattrNames(path string) ([]string, error) {
	sz, err := syscall.Listxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	n, err := syscall.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, part := range strings.Split(string(buf[:n]), "\x00") {
		if part != "" {
			names = append(names, part)
		}
	}
	return names, nil
}

func getXattr(path, name string) ([]byte, error) {
	sz, err := syscall.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, sz)
	n, err := syscall.Getxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func isXattrUnsupported(err error) bool {
	return errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP)
}
