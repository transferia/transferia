package mongo

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.ytsaurus.tech/library/go/core/log"
)

// after this many seconds of oplog time a pending transaction is reported as stuck
const oplogTxnStuckSeconds = 60

type oplogTxnKey struct {
	lsid      string // raw bytes of the lsid document
	txnNumber int64
}

type pendingOplogTxn struct {
	events []*KeyChangeEvent
	size   int // total size of buffered oplog entries in bytes
	// holdTS is the timestamp of the last oplog entry before this tx. While the tx is pending, persisted
	// progress must not pass holdTS: on restart the prepare entries have to be re-read.
	holdTS  primitive.Timestamp
	firstTS primitive.Timestamp
	warned  bool
}

// oplogTxnTracker buffers ops of multi-document transactions (applyOps chains) until their commit.
// Prepared (2PC) transactions put ops into the oplog at prepare time; applying them immediately is wrong:
// refetch by _id before commit reads the pre-transaction document, and deletes of an aborted tx cannot be undone.
type oplogTxnTracker struct {
	logger  log.Logger
	pending map[oplogTxnKey]*pendingOplogTxn
}

func newOplogTxnTracker(logger log.Logger) *oplogTxnTracker {
	return &oplogTxnTracker{
		logger:  logger,
		pending: map[oplogTxnKey]*pendingOplogTxn{},
	}
}

// Process converts an oplog entry into events ready to be pushed and the size in bytes they account for.
// Ops of a transaction are returned only when it commits, with the commit entry timestamp; aborted transactions
// are dropped. prevTS is the timestamp of the previous oplog entry (or the batch start timestamp).
func (t *oplogTxnTracker) Process(
	e oplogRsChangeEventV2, prevTS primitive.Timestamp, entrySize int,
) ([]*KeyChangeEvent, int, error) {
	if e.OperationType != "c" || len(e.LSID) == 0 {
		events, err := e.toMongoKeyChangeEvents(t.logger)
		return events, entrySize, err
	}
	var selector oplogRsChangeEventV2CommandSelector
	if err := bson.Unmarshal(e.Object, &selector); err != nil {
		return nil, 0, xerrors.Errorf("selector unmarshal error in operation type %s: %w", e.OperationType, err)
	}
	key := oplogTxnKey{lsid: string(e.LSID), txnNumber: e.TxnNumber}
	switch {
	case selector.CommitTransaction != 0:
		txn := t.pending[key]
		if txn == nil {
			// its prepare entries are behind the replication start point, ops of this tx may be lost
			t.logger.Error("Commit of an unknown prepared transaction",
				log.String("lsid", fmt.Sprintf("%x", e.LSID)),
				log.Int64("txnNumber", e.TxnNumber),
				log.Any("commitTS", e.Timestamp))
			return nil, 0, nil
		}
		delete(t.pending, key)
		return withClusterTime(txn.events, e.Timestamp), txn.size + entrySize, nil
	case selector.AbortTransaction != 0:
		delete(t.pending, key)
		return nil, 0, nil
	case selector.Prepare || selector.PartialTxn || len(selector.ApplyOps) > 0:
		// prepared tx of a 2PC participant without writes has an empty applyOps, hence the flags in condition
		events, err := e.applyOpsToKeyChangeEvents(t.logger, selector.ApplyOps)
		if err != nil {
			return nil, 0, xerrors.Errorf("cannot expand transaction applyOps: %w", err)
		}
		txn := t.pending[key]
		if selector.PartialTxn || selector.Prepare {
			if txn == nil {
				txn = &pendingOplogTxn{events: nil, size: 0, holdTS: prevTS, firstTS: e.Timestamp, warned: false}
				t.pending[key] = txn
			}
			txn.events = append(txn.events, events...)
			txn.size += entrySize
			return nil, 0, nil
		}
		// applyOps without prepare/partialTxn is the commit of an unprepared tx
		if txn != nil {
			delete(t.pending, key)
			events = append(txn.events, events...)
			entrySize += txn.size
		}
		return withClusterTime(events, e.Timestamp), entrySize, nil
	default:
		events, err := e.toMongoKeyChangeEvents(t.logger)
		if err != nil {
			return nil, 0, xerrors.Errorf("cannot convert command entry: %w", err)
		}
		return events, entrySize, nil
	}
}

// HoldTS returns the timestamp which persisted progress must not pass while transactions are pending.
// current is the timestamp of the entry being processed, used to detect stuck transactions.
func (t *oplogTxnTracker) HoldTS(current primitive.Timestamp) (primitive.Timestamp, bool) {
	var hold primitive.Timestamp
	held := false
	for key, txn := range t.pending {
		if !held || txn.holdTS.Compare(hold) < 0 {
			hold, held = txn.holdTS, true
		}
		if !txn.warned && current.T > txn.firstTS.T+oplogTxnStuckSeconds {
			txn.warned = true
			t.logger.Warn("Transaction is pending for too long, replication progress is held back",
				log.String("lsid", fmt.Sprintf("%x", key.lsid)),
				log.Int64("txnNumber", key.txnNumber),
				log.Any("firstTS", txn.firstTS))
		}
	}
	return hold, held
}

func withClusterTime(events []*KeyChangeEvent, ts primitive.Timestamp) []*KeyChangeEvent {
	for _, event := range events {
		event.ClusterTime = ts
	}
	return events
}
