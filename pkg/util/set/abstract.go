package set

type AbstractSet[T any] interface {
	Add(values ...T)
	Remove(values ...T)
	Len() int
	Empty() bool
	Contains(value T) bool
	Range(callback func(value T))
	String() string
	Slice() []T
	SortedSliceFunc(less func(a, b T) bool) []T
	Without(toExclude AbstractSet[T]) []T
	Equals(o AbstractSet[T]) bool
}
