package snapshot

import (
	"testing"
	"time"
)

func TestStoreSetGet(t *testing.T) {
	s := newStore(10, 0)

	if _, ok := s.Get("a"); ok {
		t.Fatal("expected miss on empty store")
	}

	s.Set("a", 1, []byte(`1`))
	s.Set("a", 1, []byte(`2`))

	got, ok := s.Get("a")
	if !ok || string(got) != "2" {
		t.Fatalf("expected latest value 2, got %q (ok=%v)", got, ok)
	}
}

func TestStoreCopiesData(t *testing.T) {
	s := newStore(10, 0)
	buf := []byte(`abc`)
	s.Set("a", 1, buf)
	buf[0] = 'X'

	got, _ := s.Get("a")
	if string(got) != "abc" {
		t.Fatalf("store must not alias caller buffer, got %q", got)
	}
	got[0] = 'Y'
	again, _ := s.Get("a")
	if string(again) != "abc" {
		t.Fatalf("Get must return a copy, got %q", again)
	}
}

func TestStoreDeleteOwnedOnlyRemovesOwnEntry(t *testing.T) {
	s := newStore(10, 0)
	s.Set("a", 1, []byte(`old`))
	s.Set("a", 2, []byte(`new`)) // a newer stream took over the key

	s.DeleteOwned("a", 1) // old stream closes late
	if got, ok := s.Get("a"); !ok || string(got) != "new" {
		t.Fatalf("old stream must not delete the newer snapshot, got %q ok=%v", got, ok)
	}

	s.DeleteOwned("a", 2)
	if _, ok := s.Get("a"); ok {
		t.Fatal("owner should be able to delete its entry")
	}
}

func TestStoreLRUEviction(t *testing.T) {
	s := newStore(2, 0)
	s.Set("a", 1, []byte(`a`))
	s.Set("b", 1, []byte(`b`))
	s.Get("a") // a becomes most recently used
	s.Set("c", 1, []byte(`c`))

	if _, ok := s.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected a to survive")
	}
	if s.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", s.Len())
	}
}

func TestStoreTTL(t *testing.T) {
	now := time.Unix(0, 0)
	s := newStore(10, time.Minute)
	s.now = func() time.Time { return now }

	s.Set("a", 1, []byte(`a`))
	now = now.Add(30 * time.Second)
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected hit before ttl")
	}
	now = now.Add(2 * time.Minute)
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected miss after ttl")
	}
	if s.Len() != 0 {
		t.Fatal("expired entry should be removed on read")
	}
}
