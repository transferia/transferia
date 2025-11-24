package clickhouse

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	ErrNotSupportExactlyOnce = xerrors.NewSentinel("no offset in data, exactly once not supported")
)

type deduper struct {
	lgr   log.Logger
	sink  abstract.Sinker
	store InsertBlockStore
	part  abstract.TablePartID
}

func (m *deduper) Process(batch []abstract.ChangeItem) func() error {
	return func() error {
		lastBlock, lastStatus, err := m.store.Get(m.part)
		if err != nil {
			return xerrors.Errorf("unable to get prev block: %w", err)
		}
		currMin, ok := batch[0].Offset()
		if !ok {
			return abstract.NewFatalError(ErrNotSupportExactlyOnce)
		}
		currMax, ok := batch[len(batch)-1].Offset()
		if !ok {
			return abstract.NewFatalError(ErrNotSupportExactlyOnce)
		}
		currBlock := &InsertBlock{
			min: currMin,
			max: currMax,
		}
		if lastBlock == nil {
			// we know that old insert no exists
			if err := m.store.Set(m.part, currBlock, InsertBlockStatusBefore); err != nil {
				return xerrors.Errorf("unable to store current block `before`: %s: %s: %w", m.part.FqtnWithPartID(), currBlock, err)
			}
			if err := m.sink.Push(batch); err != nil {
				return xerrors.Errorf("unable to push batch: %s: %s: %w", m.part, currBlock, err)
			}
			if err := m.store.Set(m.part, currBlock, InsertBlockStatusAfter); err != nil {
				return xerrors.Errorf("unable to store current block `after`: %s: %s: %w", m.part.FqtnWithPartID(), currBlock, err)
			}
			return nil
		}

		skip, retry, insert := m.splitBatch(batch, lastBlock)
		if len(skip) > 0 {
			m.lgr.Warnf("Deduplication found %s items to skip", len(skip))
		}
		if len(retry) > 0 && lastStatus == InsertBlockStatusBefore {
			if len(retry) != int(lastBlock.max-lastBlock.min) {
				return abstract.NewFatalError(xerrors.Errorf("retry block not match, data carruption: %s: %s", m.part.FqtnWithPartID(), lastBlock))
			}
			// we do not know what happened with old insert
			if err := m.sink.Push(retry); err != nil {
				return xerrors.Errorf("unable to retry last block: %s: %s: %w", m.part.FqtnWithPartID(), lastBlock, err)
			}
			if err := m.store.Set(m.part, lastBlock, InsertBlockStatusAfter); err != nil {
				return xerrors.Errorf("unable to store current block `before`: %s: %s: %w", m.part.FqtnWithPartID(), lastBlock, err)
			}
		}
		if len(insert) > 0 {
			currBlock.min, _ = insert[0].Offset()
			if err := m.store.Set(m.part, currBlock, InsertBlockStatusBefore); err != nil {
				return xerrors.Errorf("unable to store current block `before`: %s: %s: %w", m.part.FqtnWithPartID(), currBlock, err)
			}
			if err := m.sink.Push(insert); err != nil {
				return xerrors.Errorf("unable to retry last block: %s: %s: %w", m.part, lastBlock, err)
			}
			if err := m.store.Set(m.part, currBlock, InsertBlockStatusAfter); err != nil {
				return xerrors.Errorf("unable to store current block `before`: %s: %s: %w", m.part, currBlock, err)
			}
		}
		return nil
	}
}

func (m *deduper) splitBatch(
	batch []abstract.ChangeItem,
	lastBlock *InsertBlock,
) (
	skip []abstract.ChangeItem,
	retry []abstract.ChangeItem,
	insert []abstract.ChangeItem,
) {
	for _, item := range batch {
		offset, _ := item.Offset()
		if lastBlock.min > offset {
			skip = append(skip, item)
			continue
		}
		if lastBlock.max <= offset {
			retry = append(retry, item)
			continue
		}
		insert = append(insert, item)
	}
	return
}

func newDeduper(part abstract.TablePartID, sink abstract.Sinker, store InsertBlockStore, lgr log.Logger) *deduper {
	return &deduper{
		lgr:   lgr,
		sink:  sink,
		store: store,
		part:  part,
	}
}
