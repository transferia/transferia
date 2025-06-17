//go:build !disable_postgres_provider

package sequencer

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// progressInfo responsible for grouping lsns by transactions
// it also ensures that only lsns of committed transactions are sent back to server in Standby update
// and all transactions preceding the one that we send in Standby are already processed
type progressInfo struct {
	processing     map[uint32]*lsnTransaction // transaction id -> all lsns
	transactionIDs []uint32                   // all transaction ids in correct order
	lastCommitted  uint64                     // last lsn of last processed transaction
}

func (p *progressInfo) add(tid uint32, lsn uint64) error {
	if _, ok := p.processing[tid]; !ok {
		p.processing[tid] = newLsnTransaction()
		p.transactionIDs = append(p.transactionIDs, tid)
	}
	if err := p.processing[tid].appendLsn(lsn); err != nil {
		return xerrors.Errorf("unable to add item with lsn %v: %w", lsn, err)
	}
	return nil
}

// check that this transaction is processing
func (p *progressInfo) check(tid uint32) bool {
	_, ok := p.processing[tid]
	return ok
}

func (p *progressInfo) remove(tid uint32, lsns []uint64) error {
	if !p.check(tid) {
		return xerrors.Errorf("transaction info was not added, transaction ID: %v", tid)
	}

	if err := p.processing[tid].removeLsn(lsns); err != nil {
		return err
	}
	return nil
}

func (p *progressInfo) updateCommitted() uint64 {
	// in case last transaction is not complete yet
	if len(p.transactionIDs) <= 1 {
		return p.lastCommitted
	}

	var firstIdx int
	for i, id := range p.transactionIDs[:len(p.transactionIDs)-1] {
		if len(p.processing[id].lsns) == 0 { // we are done processing this transaction
			p.lastCommitted = p.processing[id].lastLsn
			delete(p.processing, id)
			firstIdx = i + 1
		} else {
			break
		}
	}
	p.transactionIDs = p.transactionIDs[firstIdx:] // remove all processed transactions
	return p.lastCommitted
}

func newProgressInfo() *progressInfo {
	return &progressInfo{
		processing:     make(map[uint32]*lsnTransaction, 0),
		transactionIDs: make([]uint32, 0),
		lastCommitted:  0,
	}
}
