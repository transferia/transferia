//go:build !disable_greenplum_provider

package greenplum

import (
	"math/rand"
)

// SegPointerPool is a set of Greenplum storage segment pointers with additional functions
type SegPointerPool struct {
	// pool is a set of segments this sink uses to INSERT data to. Is initialized at the first push of a row changeitem
	pool []GPSegPointer
	// nextRoundRobinIndex is the next position in the pool for round-robin algorithm
	nextRoundRobinIndex int
}

// NewRandomSegPointerPool constructs a pool of the given size, the first element of which is chosen randomly from a ring consisting of the given total number of segments
func NewRandomSegPointerPool(totalSegments int, size int) *SegPointerPool {
	result := &SegPointerPool{
		pool:                make([]GPSegPointer, size),
		nextRoundRobinIndex: 0,
	}

	randSegPoolStart := rand.Intn(totalSegments)
	segI := randSegPoolStart
	for i := 0; i < size; i++ {
		result.pool[i] = Segment(segI)
		segI = (segI + 1) % totalSegments
	}

	return result
}

func (p *SegPointerPool) NextRoundRobin() GPSegPointer {
	result := p.pool[p.nextRoundRobinIndex]
	p.nextRoundRobinIndex = (p.nextRoundRobinIndex + 1) % len(p.pool)
	return result
}
