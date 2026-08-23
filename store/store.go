package store

import (
	"sync"
)

type KVStore[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K)
	Len() int
	Range(fn func(K, V) bool)
}

type Store[K comparable, V any] struct { // k is type and comparable is contraint on k
	data map[K]V      // key of type k, and value of type v
	mu   sync.RWMutex // when creating object, wont specify - gets zero valu, for this valid
}

// like a constructor
func NewStore[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{data: make(map[K]V)}
}

func (s *Store[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Replace atomically swaps the store contents. The caller must not mutate data
// after passing it to Replace.
// swap entire map in one locked op
func (s *Store[K, V]) Replace(data map[K]V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
}

func (s *Store[K, V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Calls fn for each key/value pair.
// Stops iteration if fn returns false.
func (s *Store[K, V]) Range(fn func(K, V) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.data {
		if !fn(k, v) {
			break
		}
	}
}
