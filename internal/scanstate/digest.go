package scanstate

import "encoding/binary"

// Hasher folds one directory's direct entries into the 128 bit digest stored in
// a Record.
//
// It is not a cryptographic hash and does not need to be: the digest only has
// to notice that a directory's contents differ from the last verified pass. It
// is deterministic across runs and platforms, which is all a checkpoint on disk
// requires.
//
// Entries must be folded in the order os.ReadDir returns them (sorted by name),
// because that is the order the previous run used.
type Hasher struct {
	h1, h2 uint64
}

// NewHasher returns a Hasher with the FNV-1a basis mixed into two lanes.
func NewHasher() *Hasher {
	return &Hasher{h1: 0xcbf29ce484222325, h2: 0x9e3779b97f4a7c15}
}

func (h *Hasher) b(v byte) {
	h.h1 = (h.h1 ^ uint64(v)) * 0x100000001b3
	h.h2 = (h.h2 + uint64(v) + 1) * 0xff51afd7ed558ccd
	h.h2 ^= h.h2 >> 29
}

func (h *Hasher) u(v uint64) {
	for i := 0; i < 8; i++ {
		h.b(byte(v))
		v >>= 8
	}
}

// Name folds an entry name. The length is folded first so that ("ab","c") and
// ("a","bc") cannot collide by concatenation.
func (h *Hasher) Name(name string) {
	h.u(uint64(len(name)))
	for i := 0; i < len(name); i++ {
		h.b(name[i])
	}
}

// meta folds the permission word and owner. The checkpoint has to notice a
// chmod or chown made while the daemon was stopped: those do not change the
// directory mtime, and without this a held directory would be skipped and its
// target would keep the old metadata forever.
func (h *Hasher) meta(mode uint32, uid, gid int) {
	h.u(uint64(mode))
	h.u(uint64(int64(uid)))
	h.u(uint64(int64(gid)))
}

// Dir folds a subdirectory entry. Its name and its own metadata count; its
// own mtime does not, because it changes on every write below it and would
// invalidate this directory's record for the whole subtree's activity. The
// child's own record covers its content.
func (h *Hasher) Dir(mode uint32, uid, gid int) {
	h.b('d')
	h.meta(mode, uid, gid)
}

// File folds a regular file entry, size, mtime and metadata included.
//
// The mtime is what catches a file rewritten in place: that does not bump the
// parent directory's mtime, so the directory's own timestamp alone would let a
// changed file look unchanged. Mode and owner catch a chmod/chown that leaves
// the mtime alone.
func (h *Hasher) File(size, mtime int64, mode uint32, uid, gid int) {
	h.b('f')
	h.u(uint64(size))
	h.u(uint64(mtime))
	h.meta(mode, uid, gid)
}

// Other folds a symlink or special entry by its metadata. The link target is
// not available from FileInfo; replacing a link changes the parent mtime.
func (h *Hasher) Other(mode uint32, uid, gid int) {
	h.b('o')
	h.meta(mode, uid, gid)
}

// Sum returns the digest.
func (h *Hasher) Sum() [16]byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[:8], h.h1)
	binary.BigEndian.PutUint64(out[8:], h.h2)
	return out
}
