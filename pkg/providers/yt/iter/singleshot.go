//go:build !disable_yt_provider

package iter

type IteratorBase interface {
	Next() bool
	Close()
}

type singleshotIterState uint8

var (
	iterStateInitial = singleshotIterState(0)
	iterStateReady   = singleshotIterState(1)
	iterStateClosed  = singleshotIterState(2)
)

func (it *singleshotIterState) Next() bool {
	switch *it {
	case iterStateInitial:
		*it = iterStateReady
		return true
	case iterStateReady:
		*it = iterStateClosed
		return false
	default:
		return false
	}
}

func (it *singleshotIterState) Close() {
	*it = iterStateClosed
}

func NewSingleshotIter() IteratorBase {
	it := new(singleshotIterState)
	*it = iterStateInitial
	return it
}
