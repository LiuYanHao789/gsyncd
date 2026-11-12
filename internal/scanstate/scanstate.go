// Package scanstate remembers what an initial copy has already verified.
//
// Restarting a mirror of ten terabytes used to mean walking the whole source
// tree again before the first byte moved: 50 million files at the measured
// 2,336 files/second is close to six hours of pure metadata reads. The store
// records, per source directory, that directory's mtime and a digest of its
// direct entries - names, and for files their size and mtime. On the next start
// the walk still reads every entry, because one stat per file is the only way
// to see a file that changed while the process was down, but a directory whose
// mtime and digest still match needs no target-side work at all: no readdir, no
// per-file stat, no prune.
//
// The store is an accelerator, never evidence. A missing, unreadable, torn or
// stale record makes the walk do more work, never less; every failure path in
// this package degrades to "no checkpoint".
package scanstate

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const magic = "GSYNCD-SCAN-1\n"

// sealMagic terminates a segment written by a run that reached the end of the
// walk. A killed run's .tmp has records but no seal, and must never be adopted:
// its records can describe directories whose files were still queued when the
// process died.
const sealMagic = "GSYNCD-SEAL-1\n"

var errShort = errors.New("scanstate: truncated record")

// Record is what one directory looked like when its copy was verified.
type Record struct {
	// Path is relative to the sync root, slash separated, "." for the root.
	Path string
	// Mtime is the directory's own modification time in Unix nanoseconds.
	// Creating, removing or renaming an entry inside a directory bumps it, so a
	// changed mtime is proof that the entry set moved.
	Mtime int64
	// Count is the number of direct entries the digest covers.
	Count uint32
	// Hash is a digest over the direct entries: mode, uid and gid for every
	// entry, plus name; a regular file also folds size and mtime.
	//
	// A subdirectory's own mtime is deliberately excluded. Including it would
	// make one write in one leaf invalidate every ancestor up to the root, and
	// the checkpoint would then never match on a tree that is being written to.
	// The child's own record covers the child.
	Hash [16]byte
}

// Store is one checkpoint file. It is safe to use from the walking goroutine
// only; the copy workers never touch it.
type Store struct {
	path string
	tmp  string
	fp   string
	logf func(level string, format string, args ...any)

	// write side
	f       *os.File
	w       *bufio.Writer
	scratch []byte
	written uint64
	begun   bool
	failed  bool

	// read side
	r    *os.File
	rb   *bufio.Reader
	cur  Record
	have bool
	done bool
	used uint64

	mu sync.Mutex
}

// Open prepares a store at path. It never fails: anything wrong with the
// existing file simply means the next walk verifies everything.
func Open(path, fingerprint string, logf func(string, string, ...any)) *Store {
	if logf == nil {
		logf = func(string, string, ...any) {}
	}
	return &Store{path: path, tmp: path + ".tmp", fp: fingerprint, logf: logf}
}

// Begin rewinds the reader and starts a fresh write segment.
//
// It is called at the start of every initial copy, so a full resync started by
// a queue overflow re-reads the checkpoint rather than the tail of the previous
// pass.
func (s *Store) Begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeReader()
	s.have, s.done, s.used = false, false, 0
	s.failed = false
	s.begun = false
	s.openReader()

	f, err := openScanTemp(s.tmp)
	if err != nil {
		s.closeWriter()
		return err
	}
	s.f = f
	s.w = bufio.NewWriterSize(f, 1<<16)
	s.written = 0
	s.begun = true
	if _, err := s.w.WriteString(magic + s.fp + "\n"); err != nil {
		s.failed = true
		s.closeWriter()
		return err
	}
	return nil
}

// openScanTemp creates the segment with O_EXCL|O_NOFOLLOW. The name is fixed
// (it is the crash-recovery marker), so following a planted symlink here let a
// local user make a root daemon truncate an arbitrary file.
func openScanTemp(path string) (*os.File, error) {
	for attempt := 0; attempt < 3; attempt++ {
		f, err := openScanSegment(path)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		st, lerr := os.Lstat(path)
		if lerr != nil {
			continue
		}
		// A stale regular segment from a killed run is safe to replace. A
		// symlink or any other object is not something we wrote.
		if !st.Mode().IsRegular() {
			_ = os.Remove(path)
			continue
		}
		_ = os.Remove(path)
	}
	return nil, fmt.Errorf("cannot create scan segment %s", path)
}

// Lookup returns the record for rel if the previous run reached it.
//
// The reader is a forward-only cursor: the walk visits directories in the same
// deterministic order the previous run recorded them, so the merge is O(1) in
// memory instead of a map holding one entry per directory (which on a fifty
// million directory tree would be gigabytes of RAM).
func (s *Store) Lookup(rel string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rb == nil {
		return Record{}, false
	}
	for {
		if !s.have {
			rec, ok := s.next()
			if !ok {
				return Record{}, false
			}
			s.cur, s.have = rec, true
		}
		if s.cur.Path == rel {
			s.used++
			return s.cur, true
		}
		if walkLess(rel, s.cur.Path) {
			// rel sorts before the cursor: it did not exist last time.
			return Record{}, false
		}
		s.have = false
	}
}

// Add buffers one verified directory.
func (s *Store) Add(rec Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return
	}
	body := s.encode(&rec)
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(body)))
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	if _, err := s.w.Write(lenBuf[:n]); err != nil {
		s.failed = true
		s.closeWriter()
		return
	}
	if _, err := s.w.Write(body); err != nil {
		s.failed = true
		s.closeWriter()
		return
	}
	if _, err := s.w.Write(crc[:]); err != nil {
		s.failed = true
		s.closeWriter()
		return
	}
	s.written++
}

// Commit hands the buffered records to the kernel.
//
// It deliberately does not fsync. Every record describes a directory whose
// copies have already completed, so the only thing a lost record costs is
// re-verification on the next start — the safe direction. Paying an fsync per
// few thousand records instead cost more than the verification it was saving:
// on a 210k directory tree, 51 syncs turned a 23s warm pass into 45s.
//
// The durability that does matter is the final rename in Finish, and that one
// is synced.
func (s *Store) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		s.failed = true
		s.closeWriter()
		return err
	}
	return nil
}

// Finish closes the store. complete reports whether the walk reached the end of
// the tree; only then is the new segment promoted over the old one. An
// interrupted run keeps its partial segment, which the next Open picks up when
// no complete segment exists.
func (s *Store) Finish(complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w != nil {
		if complete && s.begun {
			if _, err := s.w.WriteString(sealMagic); err != nil {
				complete = false
			}
		}
		if err := s.w.Flush(); err != nil {
			complete = false
			s.failed = true
		} else {
			_ = s.f.Sync()
		}
		s.closeWriter()
	}
	s.closeReader()
	if !complete || !s.begun || s.failed {
		// Begin failed: there is no new segment in this run. Removing s.path
		// here would destroy the last good checkpoint and could promote an
		// unrelated stale .tmp.
		s.begun = false
		return
	}
	s.begun = false
	s.failed = false
	if err := os.Rename(s.tmp, s.path); err != nil {
		s.logf("debug", "scan state: cannot promote %s: %v", s.tmp, err)
	}
}

// Verified reports how many directories the previous run vouched for.
func (s *Store) Verified() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// Path is where the checkpoint lives, for log lines.
func (s *Store) Path() string { return s.path }

func (s *Store) closeWriter() {
	if s.w != nil {
		_ = s.w.Flush()
	}
	if s.f != nil {
		_ = s.f.Close()
	}
	s.w, s.f = nil, nil
}

func (s *Store) closeReader() {
	if s.r != nil {
		_ = s.r.Close()
	}
	s.r, s.rb = nil, nil
}

func (s *Store) openReader() {
	if f, br, ok := s.tryOpen(s.path); ok {
		s.r, s.rb = f, br
		return
	}
	// No complete segment. Only adopt a .tmp that carries the end-of-walk
	// seal: a killed run's segment had records written before its files were
	// copied, and adopting it could make the next pass skip those files.
	if st, err := os.Lstat(s.tmp); err != nil || !st.Mode().IsRegular() {
		return
	}
	if !hasSeal(s.tmp) {
		return
	}
	_ = os.Remove(s.path)
	if err := os.Rename(s.tmp, s.path); err != nil {
		return
	}
	if f, br, ok := s.tryOpen(s.path); ok {
		s.r, s.rb = f, br
	}
}

func (s *Store) tryOpen(path string) (*os.File, *bufio.Reader, bool) {
	if !hasSeal(path) {
		return nil, nil, false
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, false
	}
	br := bufio.NewReaderSize(f, 1<<16)
	// The header is two short lines: the format marker, then the fingerprint
	// of the configuration the records belong to. Bound both reads: a damaged
	// or hostile file with no newline must not allocate the whole file before
	// the record-size cap applies.
	line, ok := scanHeaderLine(br)
	if !ok || line != magic {
		_ = f.Close()
		return nil, nil, false
	}
	fp, ok := scanHeaderLine(br)
	if !ok || fp != s.fp+"\n" {
		// A different source, target, exclude list or binary: the records
		// describe a tree this run is not walking.
		_ = f.Close()
		return nil, nil, false
	}
	return f, br, true
}

// hasSeal reports whether path ends with the complete-pass marker. Old
// checkpoints from before the seal existed are ignored (one extra full walk,
// then a sealed segment is written).
func hasSeal(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() < int64(len(sealMagic)) {
		return false
	}
	buf := make([]byte, len(sealMagic))
	if _, err := f.ReadAt(buf, st.Size()-int64(len(sealMagic))); err != nil {
		return false
	}
	return string(buf) == sealMagic
}

// scanHeaderLine reads one newline-terminated header line with a hard cap.
func scanHeaderLine(br *bufio.Reader) (string, bool) {
	const maxHeader = 4096
	var buf []byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			return "", false
		}
		buf = append(buf, b)
		if len(buf) > maxHeader {
			return "", false
		}
		if b == '\n' {
			return string(buf), true
		}
	}
}

func (s *Store) next() (Record, bool) {
	if s.done || s.rb == nil {
		return Record{}, false
	}
	n, err := binary.ReadUvarint(s.rb)
	if err != nil {
		s.done = true
		return Record{}, false
	}
	if n > 1<<24 {
		s.done = true
		return Record{}, false
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(s.rb, body); err != nil {
		s.done = true
		return Record{}, false
	}
	var crc [4]byte
	if _, err := io.ReadFull(s.rb, crc[:]); err != nil {
		s.done = true
		return Record{}, false
	}
	if binary.LittleEndian.Uint32(crc[:]) != crc32.ChecksumIEEE(body) {
		// Torn tail: everything after this point is untrustworthy.
		s.done = true
		return Record{}, false
	}
	rec, err := decodeRecord(body)
	if err != nil {
		s.done = true
		return Record{}, false
	}
	return rec, true
}

func (s *Store) encode(rec *Record) []byte {
	b := s.scratch[:0]
	b = binary.AppendUvarint(b, uint64(len(rec.Path)))
	b = append(b, rec.Path...)
	b = binary.AppendVarint(b, rec.Mtime)
	b = binary.AppendUvarint(b, uint64(rec.Count))
	b = append(b, rec.Hash[:]...)
	s.scratch = b
	return b
}

func decodeRecord(body []byte) (Record, error) {
	var rec Record
	n, m := binary.Uvarint(body)
	if m <= 0 || uint64(len(body)-m) < n {
		return rec, errShort
	}
	rec.Path = string(body[m : m+int(n)])
	body = body[m+int(n):]
	mt, m := binary.Varint(body)
	if m <= 0 {
		return rec, errShort
	}
	rec.Mtime = mt
	body = body[m:]
	c, m := binary.Uvarint(body)
	if m <= 0 {
		return rec, errShort
	}
	rec.Count = uint32(c)
	body = body[m:]
	if len(body) < 16 {
		return rec, errShort
	}
	copy(rec.Hash[:], body[:16])
	return rec, nil
}

// walkLess reports whether a comes before b in the order filepath.Walk visits
// directories: a parent before its children, siblings in byte order.
//
// Plain string comparison is not that order. "a-b" sorts before "a/b" because
// '-' is 0x2d and '/' is 0x2f, but Walk descends into "a" and visits "a/b"
// first. Comparing component by component is what the cursor merge needs.
func walkLess(a, b string) bool {
	for {
		if a == b {
			return false
		}
		if a == "" {
			return true
		}
		if b == "" {
			return false
		}
		ca, ra := splitComponent(a)
		cb, rb := splitComponent(b)
		if ca != cb {
			return ca < cb
		}
		a, b = ra, rb
	}
}

func splitComponent(p string) (string, string) {
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			return p[:i], p[i+1:]
		}
	}
	return p, ""
}

// Fingerprint identifies the configuration a checkpoint belongs to. Anything
// that changes which files are copied or where they go must change it, because
// the records are only meaningful for one tree and one target.
func Fingerprint(source, target string, excludes []string, extra ...string) string {
	h := crc32.NewIEEE()
	for _, part := range []string{magic, source, "\x00", target, "\x00"} {
		_, _ = h.Write([]byte(part))
	}
	for _, e := range excludes {
		_, _ = h.Write([]byte(e))
		_, _ = h.Write([]byte{0})
	}
	for _, e := range extra {
		_, _ = h.Write([]byte(e))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%08x", h.Sum32())
}
