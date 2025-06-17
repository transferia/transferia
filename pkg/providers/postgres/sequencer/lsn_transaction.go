//go:build !disable_postgres_provider

package sequencer

import "github.com/transferia/transferia/library/go/core/xerrors"

// lsnTransaction contains all message lsns and ensures correct order
type lsnTransaction struct {
	lastLsn uint64   // last message lsn that was received
	lsns    []uint64 // all the message lsns that were added, must go in increasing order
}

// appendLsn checks that we append lsns in nondecreasing order only.
// same lsn may appear in a transaction when COPY TO/FROM is used
// This is required because we read messages from server synchronously
func (p *lsnTransaction) appendLsn(newLsn uint64) error {
	if p.lastLsn > newLsn {
		return xerrors.Errorf("tried to add lsns in decreasing order. Last lsn: %v, new lsn: %v", p.lastLsn, newLsn)
	}
	p.lastLsn = newLsn
	p.lsns = append(p.lsns, newLsn)

	return nil
}

// here we go through lsns and add to the new slice those that were not found in pushedLsns
// we also check that we only try to remove lsns that were previously added
// objects in pushedLsns must go in increasing order
func (p *lsnTransaction) removeLsn(pushedLsns []uint64) error {
	if err := checkOrder(pushedLsns); err != nil {
		return err
	}

	newLsns := make([]uint64, 0, len(p.lsns))
	pointer := 0
	for i, lsn := range p.lsns {
		if pointer == len(pushedLsns) {
			newLsns = append(newLsns, p.lsns[i:]...)
			break
		}

		if lsn == pushedLsns[pointer] {
			pointer++ // found lsn to remove, don't add it to new slice
		} else {
			newLsns = append(newLsns, lsn)
		}
	}

	if len(newLsns)+len(pushedLsns) != len(p.lsns) {
		return xerrors.New("Tried to push unknown lsns. you should push only taken lsns")
	}

	p.lsns = newLsns
	return nil
}

func newLsnTransaction() *lsnTransaction {
	return &lsnTransaction{
		lsns:    make([]uint64, 0),
		lastLsn: 0,
	}
}

func checkOrder(lsns []uint64) error {
	var last uint64
	for _, lsn := range lsns {
		if lsn < last {
			return xerrors.New("the order of lsns is broken")
		}
		last = lsn
	}
	return nil
}
