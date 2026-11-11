package engine

import (
	"context"
	"testing"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// A rename with only one side excluded must become a Create or a Delete, not
// a rename. Passing the pair through would either copy excluded content onto
// the target or recreate it under an excluded destination, which is exactly
// what the filter is supposed to prevent.
func TestFilteredMoveIsSplit(t *testing.T) {
	be := &fakeBackend{}
	e, _ := newTestEngine(t, be, 0, []string{"/excluded/"})

	// Excluded source, included destination: the mirror only sees a new file.
	e.Handle(event.Event{Path: "/src/ok/from-excluded", From: "/src/excluded/f", Op: event.Move})
	// Included source, excluded destination: the mirror only sees a delete.
	e.Handle(event.Event{Path: "/src/excluded/g", From: "/src/ok/to-excluded", Op: event.Move})
	// Both sides excluded: nothing at all.
	e.Handle(event.Event{Path: "/src/excluded/h", From: "/src/excluded/h2", Op: event.Move})
	// A directory moved out of the excluded tree needs a whole-subtree copy.
	e.Handle(event.Event{Path: "/src/ok/dir", From: "/src/excluded/dir", Op: event.Move, IsDir: true})

	e.FlushAll(context.Background())
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("want one batch, got %d", len(got))
	}
	changes := got[0].Changes
	if len(changes) != 3 {
		t.Fatalf("want 3 changes, got %d: %+v", len(changes), changes)
	}
	var sawCreate, sawDelete, sawSubtree bool
	for _, c := range changes {
		if c.Path == "/src/ok/from-excluded" {
			if c.Op&event.Create == 0 {
				t.Fatalf("excluded -> included was not a Create: %+v", c)
			}
			sawCreate = true
		}
		if c.Path == "/src/ok/to-excluded" {
			if c.Op&event.Delete == 0 {
				t.Fatalf("included -> excluded was not a Delete: %+v", c)
			}
			sawDelete = true
		}
		if c.Path == "/src/ok/dir" {
			if !c.Subtree || c.Kind != aggregate.KindDir {
				t.Fatalf("directory split did not request a subtree copy: %+v", c)
			}
			sawSubtree = true
		}
	}
	if !sawCreate || !sawDelete || !sawSubtree {
		t.Fatalf("missing split result: create=%v delete=%v subtree=%v changes=%+v",
			sawCreate, sawDelete, sawSubtree, changes)
	}
}
