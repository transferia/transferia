package arena

import "unsafe"

// arena is a chunked typed slot buffer. Values are appended into the current
// chunk; when it fills, a fresh chunk is allocated. Pointers previously handed
// out keep old chunks alive; GC reclaims each chunk once none of its slots are
// referenced downstream.
type arena[T any] struct {
	buf      []T
	pos      int
	chunkCap int
}

func newArena[T any](chunkCap int) *arena[T] {
	return &arena[T]{
		buf:      make([]T, chunkCap),
		pos:      0,
		chunkCap: chunkCap,
	}
}

func (a *arena[T]) put(v T) unsafe.Pointer {
	if a.pos >= len(a.buf) {
		a.buf = make([]T, a.chunkCap)
		a.pos = 0
	}
	a.buf[a.pos] = v
	p := unsafe.Pointer(&a.buf[a.pos])
	a.pos++
	return p
}
