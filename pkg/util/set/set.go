package set

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

type Set[T comparable] struct {
	values map[T]struct{}
}

var _ AbstractSet[int] = (*Set[int])(nil)

func New[T comparable](values ...T) *Set[T] {
	result := &Set[T]{values: make(map[T]struct{}, len(values))}
	result.Add(values...)
	return result
}

func (s *Set[T]) Add(values ...T) {
	for _, value := range values {
		s.values[value] = struct{}{}
	}
}

func (s *Set[T]) Remove(values ...T) {
	for _, value := range values {
		delete(s.values, value)
	}
}

func (s *Set[T]) Len() int {
	return len(s.values)
}

func (s *Set[T]) Empty() bool {
	return s.Len() == 0
}

func (s *Set[T]) Contains(value T) bool {
	_, ok := s.values[value]
	return ok
}

func (s *Set[T]) Range(callback func(value T)) {
	for value := range s.values {
		callback(value)
	}
}

func (s *Set[T]) String() string {
	sb := strings.Builder{}
	sb.WriteString("[")
	first := true
	for value := range s.values {
		if !first {
			sb.WriteString(" ")
		} else {
			first = false
		}
		sb.WriteString(fmt.Sprint(value))
	}
	sb.WriteString("]")
	return sb.String()
}

func (s *Set[T]) Slice() []T {
	result := make([]T, 0, s.Len())
	for value := range s.values {
		result = append(result, value)
	}
	return result
}

func (s *Set[T]) SortedSliceFunc(less func(a, b T) bool) []T {
	result := s.Slice()
	slices.SortFunc(result, func(a, b T) int {
		if less(a, b) {
			return -1
		}
		return 1
	})
	return result
}

// Without returns slice representing elements of current Set without elements of input.
func (s *Set[T]) Without(toExclude AbstractSet[T]) []T {
	var result []T
	for value := range s.values {
		if !toExclude.Contains(value) {
			result = append(result, value)
		}
	}
	return result
}

func (s *Set[T]) Equals(o AbstractSet[T]) bool {
	if s.Len() != o.Len() {
		return false
	}
	for v := range s.values {
		if !o.Contains(v) {
			return false
		}
	}
	return true
}
