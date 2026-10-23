package snapshot

import (
	"container/list"
	"slices"
	"sync"
	"time"
)

// store is a concurrency-safe LRU of the last payload seen for each
// subscription key, bounded by entry count and by total payload bytes.
//
// Every entry records which upstream stream (owner) wrote it, so a stream that
// is shutting down can only remove its own entry and never one written by a
// newer stream for the same key.
type store struct {
	mu         sync.Mutex
	ttl        time.Duration // 0 = never expires
	maxEntries int           // <= 0 = unbounded
	maxBytes   int           // <= 0 = unbounded
	bytes      int
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

func newStore(maxEntries, maxBytes int, ttl time.Duration) *store {
	return &store{
		ttl:        ttl,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		ll:         list.New(),
		items:      make(map[string]*list.Element),
		now:        time.Now,
	}
}

// Set stores a copy of data as the latest payload for key, written by owner.
// A payload larger than the whole byte budget is not stored, and any previous
// entry for key is removed so it cannot go stale.
func (s *store) Set(key string, owner uint64, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.maxBytes > 0 && len(data) > s.maxBytes {
		if el, ok := s.items[key]; ok {
			s.removeElement(el)
		}
		return
	}

	data = slices.Clone(data)

	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry)
		s.bytes += len(data) - len(e.data)
		e.owner = owner
		e.data = data
		e.updatedAt = s.now()
		s.ll.MoveToFront(el)
	} else {
		s.items[key] = s.ll.PushFront(&entry{key: key, owner: owner, data: data, updatedAt: s.now()})
		s.bytes += len(data)
	}

	for s.ll.Len() > 1 && s.overBudget() {
		s.removeElement(s.ll.Back())
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

// Reset drops every entry.
func (s *store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ll.Init()
	clear(s.items)
	s.bytes = 0
}

// Len returns the number of stored entries (including not yet evicted expired ones).
func (s *store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

// Bytes returns the total size of the stored payloads.
func (s *store) Bytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (s *store) overBudget() bool {
	return (s.maxEntries > 0 && s.ll.Len() > s.maxEntries) ||
		(s.maxBytes > 0 && s.bytes > s.maxBytes)
}

func (s *store) removeElement(el *list.Element) {
	e := el.Value.(*entry)
	s.ll.Remove(el)
	delete(s.items, e.key)
	s.bytes -= len(e.data)
}
