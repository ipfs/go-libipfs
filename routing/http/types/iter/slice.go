package iter

import "sync"

// FromSlice returns an iterator over the given slice.
func FromSlice[T any](s []T) *SliceIter[T] {
	return &SliceIter[T]{Slice: s}
}

type SliceIter[T any] struct {
	Slice []T
	i     int
	m     sync.Mutex
}

func (s *SliceIter[T]) Next() (T, bool, error) {
	var val T
	if s.i >= len(s.Slice) {
		return val, false, nil
	}
	val = s.Slice[s.i]
	s.i++
	return val, true, nil
}
