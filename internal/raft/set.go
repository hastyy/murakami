package raft

type Set[T comparable] map[T]struct{}

func NewSet[T comparable](size int) Set[T] {
	return make(Set[T], size)
}

// Idempotent
func (s Set[T]) Add(elem T) {
	s[elem] = struct{}{}
}

// The size of the set
func (s Set[T]) Cardinality() int {
	return len(s)
}

// Clear removes all elements from the set
func (s Set[T]) Clear() {
	for k := range s {
		delete(s, k)
	}
}
