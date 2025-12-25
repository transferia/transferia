package set

import "sync"

type SyncSet[T comparable] struct {
	mutex sync.Mutex
	impl  *Set[T]
}

var _ AbstractSet[int] = (*SyncSet[int])(nil)

func NewSyncSet[T comparable](values ...T) *SyncSet[T] {
	return &SyncSet[T]{
		mutex: sync.Mutex{},
		impl:  New(values...),
	}
}

func (s *SyncSet[T]) Add(values ...T) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.impl.Add(values...)
}

func (s *SyncSet[T]) Remove(values ...T) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.impl.Remove(values...)
}

func (s *SyncSet[T]) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Len()
}

func (s *SyncSet[T]) Empty() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Empty()
}

func (s *SyncSet[T]) Contains(value T) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Contains(value)
}

func (s *SyncSet[T]) Range(callback func(value T)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.impl.Range(callback)
}

func (s *SyncSet[T]) String() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.String()
}

func (s *SyncSet[T]) Slice() []T {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Slice()
}

func (s *SyncSet[T]) SortedSliceFunc(less func(a, b T) bool) []T {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.SortedSliceFunc(less)
}

func (s *SyncSet[T]) Without(toExclude AbstractSet[T]) []T {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Without(toExclude)
}

func (s *SyncSet[T]) Equals(o AbstractSet[T]) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.impl.Equals(o)
}
