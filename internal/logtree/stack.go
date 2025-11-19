package logtree

// stack is a simple stack implementation.
// It is used for LogTree traversal operations.
type stack[T any] struct {
	slice []T
}

func newStack[T any]() *stack[T] {
	return &stack[T]{}
}

func (s *stack[T]) Push(val T) {
	s.slice = append(s.slice, val)
}

func (s *stack[T]) Pop() T {
	val := s.slice[len(s.slice)-1]
	s.slice = s.slice[:len(s.slice)-1]
	return val
}

func (s *stack[T]) Peek() T {
	return s.slice[len(s.slice)-1]
}

func (s *stack[T]) IsEmpty() bool {
	return len(s.slice) == 0
}
