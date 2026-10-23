package snapshot

import (
	"container/list"
	"slices"
	"sync"
	"time"
)

// store is a concurrency-safe, size-bounded LRU of the last payload seen for
// each subscription key. Every entry records which upstream stream (owner)
// wrote it, so a stream that is shutting down can only remove its own entry
// and never one written by a newer stream for the same key.
type store struct {
	mu         sync.Mutex
	ttl        time.Duration // 0 = never expires
	maxEntries int           // <= 0 = unbounded
	ll         *list.List
	items      map[string]*list.Element
	now        func() time.Time
}

type entry struct {
	key       string
	owner     uint64
	data      []byte
	updatedAt time.Time
}

func newStore(maxEntries int, ttl time.Duration) *store {
	return &store{
		ttl:        ttl,
		maxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
		now:        time.Now,
	}
}

// Set stores a copy of data as the latest payload for key, written by owner.
func (s *store) Set(key string, owner uint64, data []byte) {
	data = slices.Clone(data)

	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry)
		e.owner = owner
		e.data = data
		e.updatedAt = s.now()
		s.ll.MoveToFront(el)
		return
	}

	s.items[key] = s.ll.PushFront(&entry{key: key, owner: owner, data: data, updatedAt: s.now()})

	if s.maxEntries > 0 {
		for s.ll.Len() > s.maxEntries {
			s.removeElement(s.ll.Back())
		}
	}
}

// Get returns a copy of the latest payload for key, if present and not expired.
func (s *store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.items[key]
	if !ok {
		return nil, false
	}

	e := el.Value.(*entry)
	if s.ttl > 0 && s.now().Sub(e.updatedAt) > s.ttl {
		s.removeElement(el)
		return nil, false
	}

	s.ll.MoveToFront(el)
	return slices.Clone(e.data), true
}

// DeleteOwned removes the entry for key only if it was last written by owner.
func (s *store) DeleteOwned(key string, owner uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[key]; ok && el.Value.(*entry).owner == owner {
		s.removeElement(el)
	}
}

// Len returns the number of stored entries (including not yet evicted expired ones).
func (s *store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

func (s *store) removeElement(el *list.Element) {
	s.ll.Remove(el)
	delete(s.items, el.Value.(*entry).key)
}
