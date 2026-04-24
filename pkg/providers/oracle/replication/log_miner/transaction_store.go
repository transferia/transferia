package log_miner

import (
	"container/list"
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"go.ytsaurus.tech/library/go/core/log"
)

// Store transactions rows and start times
type transactionStore struct {
	ctx          context.Context
	logger       log.Logger
	starts       *list.List
	transactions map[string]*transactionData
}

// Store single transactions data
type transactionData struct {
	ID           string
	Rows         []LogMinerRow
	StartElement *list.Element
}

// Store transaction start time to move log position for tracker
type transactionStart struct {
	Position *oracle_common.LogPosition
	IsActive bool
}

// Done transaction output for user
type FinishedTransaction struct {
	Info             *oracle_common.TransactionInfo
	Rows             []LogMinerRow
	ProgressPosition *oracle_common.LogPosition
}

func newTransactionStore(ctx context.Context, logger log.Logger) *transactionStore {
	store := &transactionStore{
		ctx:          ctx,
		logger:       logger,
		starts:       list.New(),
		transactions: map[string]*transactionData{},
	}
	go store.logState()
	return store
}

func (store *transactionStore) logState() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			frontSCN := "nil"
			frontElement := store.starts.Front()
			if frontElement != nil {
				front := frontElement.Value.(*transactionStart)
				frontSCN = fmt.Sprintf("%v", front.Position.SCN())
			}
			store.logger.Infof("Oracle transation store stats, starts len: %v, transactions count: %v, SCN: %v",
				store.starts.Len(), len(store.transactions), frontSCN)
		case <-store.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (store *transactionStore) Count() int {
	return len(store.transactions)
}

func (store *transactionStore) StartTransaction(id string, position *oracle_common.LogPosition) error {
	if _, ok := store.transactions[id]; ok {
		return xerrors.Errorf("Transaction with id '%v' already exists", id)
	}

	start := transactionStart{
		Position: position,
		IsActive: true,
	}
	startNode := store.starts.PushBack(&start)
	data := transactionData{
		ID:           id,
		Rows:         []LogMinerRow{},
		StartElement: startNode,
	}
	store.transactions[id] = &data

	return nil
}

func (store *transactionStore) AddRowToTransaction(id string, row *LogMinerRow) error {
	if data, ok := store.transactions[id]; ok {
		data.Rows = append(data.Rows, *row)
		return nil
	} else {
		return xerrors.Errorf("Can't find transaction with id '%v'", id)
	}
}

func (store *transactionStore) FinishTransaction(id string, position *oracle_common.LogPosition) (*FinishedTransaction, error) {
	if data, ok := store.transactions[id]; ok {
		delete(store.transactions, id)

		start := data.StartElement.Value.(*transactionStart)
		start.IsActive = false
		progressPosition := store.moveStarts(data.StartElement, position) // clear front
		if progressPosition == nil {
			store.clearStarts(data.StartElement) // clear back
		}

		info := oracle_common.NewTransactionInfo(data.ID, start.Position, position)

		finishedTransaction := &FinishedTransaction{
			Info:             info,
			Rows:             data.Rows,
			ProgressPosition: progressPosition,
		}
		return finishedTransaction, nil
	} else {
		return nil, xerrors.Errorf("Can't find transaction with id '%v'", id)
	}
}

func (store *transactionStore) ContainsTransaction(id string) bool {
	_, ok := store.transactions[id]
	return ok
}

func (store *transactionStore) moveStarts(startElement *list.Element, finishPosition *oracle_common.LogPosition) *oracle_common.LogPosition {
	if startElement != store.starts.Front() {
		return nil
	}

	if store.starts.Len() == 0 {
		return finishPosition
	}

	if store.starts.Len() == 1 {
		store.starts.Remove(store.starts.Front())
		return finishPosition
	}

	var position *oracle_common.LogPosition
	for {
		frontElement := store.starts.Front()
		front := frontElement.Value.(*transactionStart)
		position = front.Position
		store.starts.Remove(frontElement)

		nextElement := store.starts.Front()
		if nextElement == nil {
			return finishPosition
		}

		next := nextElement.Value.(*transactionStart)
		if next.IsActive {
			return position
		}
	}
}

func (store *transactionStore) clearStarts(startElement *list.Element) {
	for {
		prevElement := startElement.Prev()
		if prevElement == nil {
			break
		}
		prev := prevElement.Value.(*transactionStart)
		if prev.IsActive {
			break
		}
		store.starts.Remove(prevElement)
	}

	if nextElement := startElement.Next(); nextElement != nil {
		next := nextElement.Value.(*transactionStart)
		if !next.IsActive {
			store.starts.Remove(startElement)
		}
	}
}
