package watcher

import (
	"os"
	"strings"
)

// The watch table is a flat map from directory path to its slot in the parent's
// child list, plus a child index so a subtree can be enumerated without
// touching the rest of the table.
//
// Why the extra index: removing a directory has to drop every recorded
// directory below it, and "below it" used to be answered by scanning the whole
// map. One scan is cheap; the problem is that "rm -rf big" raises one Delete
// per directory, so the total cost was O(directories x directories). Measured
// on this machine before the index existed: 157us per delete at 10k watched
// directories, 1.5ms at 100k, 44ms at 1M. A million-directory removal would
// have spent about twelve hours of CPU inside the watcher, unable to drain the
// kernel queue, which is exactly how a batch delete turns into an overflow and
// a full rescan.
//
// With the child index the same operation costs the subtree, so deleting a leaf
// is O(1) and deleting a subtree is proportional to what is actually removed.

// parentOf returns the directory that contains p, or "" when p has no
// separator (a relative name, which is never attached to the tree).
func parentOf(p string) string {
	for i := len(p) - 1; i > 0; i-- {
		if p[i] == '/' || p[i] == os.PathSeparator {
			return p[:i]
		}
	}
	return ""
}

// addRef records p as watched. The caller holds t.mu.
func (t *Tree) addRef(p string) {
	if _, ok := t.dirs[p]; ok {
		return
	}
	t.dirs[p] = -1
	par := parentOf(p)
	if par == "" || par == p {
		return
	}
	kids := append(t.kids[par], p)
	t.kids[par] = kids
	t.dirs[p] = int32(len(kids) - 1)
}

// dropPath forgets p itself, leaving anything below it alone. The caller holds
// t.mu.
func (t *Tree) dropPath(p string) {
	if _, ok := t.dirs[p]; !ok {
		return
	}
	t.detachLocked(p)
	delete(t.dirs, p)
}

// detachLocked removes p from its parent's child list. The caller holds t.mu.
func (t *Tree) detachLocked(p string) {
	par := parentOf(p)
	kids := t.kids[par]
	if len(kids) == 0 {
		return
	}
	remove := func(i int) {
		last := len(kids) - 1
		if i != last {
			kids[i] = kids[last]
			// Only repair the slot of a path that is still recorded. Writing
			// unconditionally resurrects an entry that was dropped without
			// being unlinked (dropPath leaves the child list alone), and a
			// resurrected entry is one that subtree removal will never find
			// again: it is not in any child list, so it never gets deleted.
			if _, ok := t.dirs[kids[i]]; ok {
				t.dirs[kids[i]] = int32(i)
			}
		}
		kids = kids[:last]
		if len(kids) == 0 {
			delete(t.kids, par)
		} else {
			t.kids[par] = kids
		}
	}
	if slot := t.dirs[p]; slot >= 0 && int(slot) < len(kids) && kids[slot] == p {
		remove(int(slot))
		return
	}
	// The slot is stale, or the entry was recorded by hand: fall back to a scan
	// of this one parent's children, which is bounded by the fan-out.
	for i, c := range kids {
		if c == p {
			remove(i)
			return
		}
	}
}

// forgetBelowLocked drops every recorded directory strictly below root and
// returns how many entries went away. The caller holds t.mu.
func (t *Tree) forgetBelowLocked(root string) int {
	n := 0
	stack := []string{root}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, c := range t.kids[p] {
			stack = append(stack, c)
			if _, ok := t.dirs[c]; ok {
				delete(t.dirs, c)
				n++
			}
		}
		delete(t.kids, p)
	}
	return n
}

// takeBelowLocked removes root and everything recorded below it, returning the
// paths so the caller can unregister them from the backend. The caller holds
// t.mu.
func (t *Tree) takeBelowLocked(root string) []string {
	var victims []string
	if _, ok := t.dirs[root]; ok {
		victims = append(victims, root)
		t.dropPath(root)
	}
	// Breadth first so the returned order is parents before children, which
	// makes the backend's log readable and keeps Remove calls independent.
	queue := []string{root}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, c := range t.kids[p] {
			queue = append(queue, c)
			if _, ok := t.dirs[c]; ok {
				victims = append(victims, c)
				delete(t.dirs, c)
			}
		}
		delete(t.kids, p)
	}
	return victims
}

// subtreeLocked lists root (when recorded) and every recorded directory below
// it, parents first. The caller holds t.mu.
func (t *Tree) subtreeLocked(root string) []string {
	var out []string
	if _, ok := t.dirs[root]; ok {
		out = append(out, root)
	}
	queue := []string{root}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, c := range t.kids[p] {
			queue = append(queue, c)
			if _, ok := t.dirs[c]; ok {
				out = append(out, c)
			}
		}
	}
	return out
}

// rekeyDirsLocked rewrites every recorded directory below from so it reports
// under to, and returns the old paths so the caller can hand the same set to
// the backend. The caller holds t.mu.
//
// Returning the paths rather than a count is what keeps a rename cheap. The
// backend has its own path-to-descriptor table and used to answer "which
// entries are below from" by scanning all of it, which on a million watched
// directories cost 1.1s and 122MB of allocation per rename - while the event
// reader was blocked behind the same mutex. The subtree is already known here,
// for free, so the backend is told instead of asked.
func (t *Tree) rekeyDirsLocked(from, to string) []string {
	moved := t.subtreeLocked(from)
	if len(moved) == 0 {
		return nil
	}
	// Drop the whole subtree from the index first: re-inserting in the same
	// parent-before-child order rebuilds the child lists, and doing it in
	// place would make a child look like it still lives under the old name.
	if _, ok := t.dirs[from]; ok {
		t.detachLocked(from)
	}
	for _, p := range moved {
		delete(t.dirs, p)
		delete(t.kids, p)
	}
	for _, p := range moved {
		t.addRef(to + p[len(from):])
	}
	return moved
}

// OrphanCount reports recorded directories that have a parent path but are not
// listed among that parent's children. It is a diagnostic: a non-zero value
// means subtree removal would miss those entries, which is exactly the class of
// bug the index was introduced to rule out. Roots are not orphans: a path with
// no separator has no parent to be listed under.
func (t *Tree) OrphanCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for p := range t.dirs {
		par := parentOf(p)
		if par == "" {
			continue
		}
		found := false
		for _, c := range t.kids[par] {
			if c == p {
				found = true
				break
			}
		}
		if !found {
			n++
		}
	}
	return n
}

var _ = strings.TrimSuffix
