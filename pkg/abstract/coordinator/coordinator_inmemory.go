package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type CoordinatorInMemory struct {
	*CoordinatorNoOp

	mu                   sync.Mutex
	state                map[string]map[string]*TransferStateData
	taskState            map[string]string
	progress             []*abstract.OperationTablePart
	operationTablesParts map[string]*OperationTablesParts
	operationIdToWorkers map[string][]*model.OperationWorker
	// operationHealth stores last OperationHealth ping per operation and worker index (in-memory dataplane parity).
	operationHealth map[string]map[int]time.Time
}

func NewStatefulFakeClient() *CoordinatorInMemory {
	return &CoordinatorInMemory{
		CoordinatorNoOp: NewFakeClient(),

		mu:                   sync.Mutex{},
		state:                map[string]map[string]*TransferStateData{},
		taskState:            map[string]string{},
		progress:             nil,
		operationTablesParts: make(map[string]*OperationTablesParts),
		operationIdToWorkers: make(map[string][]*model.OperationWorker),
		operationHealth:      nil,
	}
}

func (f *CoordinatorInMemory) Progress() []*abstract.OperationTablePart {
	return f.progress
}

func (f *CoordinatorInMemory) GetTransferState(transferID string) (map[string]*TransferStateData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := f.state[transferID]

	logger.Log.Info("CoordinatorInMemory.GetTransferState", log.Any("transfer_id", transferID), log.Any("state", result))

	return result, nil
}

func (f *CoordinatorInMemory) SetTransferState(transferID string, state map[string]*TransferStateData) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.SetTransferState", log.Any("transfer_id", transferID), log.Any("state", state))

	if st, ok := f.state[transferID]; !ok || st == nil {
		f.state[transferID] = state
		logger.Log.Info("set transfer state", log.Any("transfer_id", transferID), log.Any("state", f.state[transferID]))
		return nil
	}
	for stateKey, stateVal := range state {
		f.state[transferID][stateKey] = stateVal
	}
	return nil
}

func (f *CoordinatorInMemory) RemoveTransferState(transferID string, stateKeys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.RemoveTransferState", log.Any("transfer_id", transferID), log.Any("state_keys", stateKeys))

	for _, stateKey := range stateKeys {
		delete(f.state[transferID], stateKey)
	}
	return nil
}

func (f *CoordinatorInMemory) SetOperationState(operationID string, state string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.SetOperationState", log.Any("operation_id", operationID), log.Any("state", state))

	f.taskState[operationID] = state
	return nil
}

func (f *CoordinatorInMemory) GetOperationState(operationID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.GetOperationState", log.Any("operation_id", operationID))

	state, ok := f.taskState[operationID]
	if !ok {
		return "", OperationStateNotFoundError
	}
	return state, nil
}

func (f *CoordinatorInMemory) CreateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.CreateOperationTablesParts", log.Any("operation_id", operationID), log.Any("tables", tables))

	operationTablesParts, ok := f.operationTablesParts[operationID]
	if !ok {
		f.operationTablesParts[operationID] = NewOperationTablesParts()
		operationTablesParts = f.operationTablesParts[operationID]
	}
	err := operationTablesParts.CreateOperationTablesParts(tables)
	if err != nil {
		return xerrors.Errorf("CreateOperationTablesParts returned error, err: %w", err)
	}
	return nil
}

func (f *CoordinatorInMemory) AssignOperationTablePart(operationID string, workerIndex int) (*abstract.OperationTablePart, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.AssignOperationTablePart", log.Any("operation_id", operationID), log.Any("workerIndex", workerIndex))

	operationTablesParts, ok := f.operationTablesParts[operationID]
	if !ok {
		f.operationTablesParts[operationID] = NewOperationTablesParts()
		operationTablesParts = f.operationTablesParts[operationID]
	}
	return operationTablesParts.AssignOperationTablePartForWorker(workerIndex), nil
}

func (f *CoordinatorInMemory) ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()

	operationTablesParts, ok := f.operationTablesParts[operationID]
	if !ok || operationTablesParts == nil {
		return 0, nil
	}
	return operationTablesParts.ClearAssignedTablesPartsForWorker(workerIndex), nil
}

// AnyOperationTablesParts returns parts for the first operation found (test helper).
func (f *CoordinatorInMemory) AnyOperationTablesParts() []*abstract.OperationTablePart {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, otp := range f.operationTablesParts {
		return otp.ListParts()
	}
	return nil
}

func (f *CoordinatorInMemory) GetOperationTablesParts(operationID string) ([]*abstract.OperationTablePart, error) {
	f.mu.Lock()
	otp := f.operationTablesParts[operationID]
	f.mu.Unlock()
	if otp == nil {
		return []*abstract.OperationTablePart{}, nil
	}
	return otp.ListParts(), nil
}

func (f *CoordinatorInMemory) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.UpdateOperationTablesParts", log.Any("operation_id", operationID), log.Any("tables", tables))

	operationTablesParts, ok := f.operationTablesParts[operationID]
	if !ok {
		f.operationTablesParts[operationID] = NewOperationTablesParts()
		operationTablesParts = f.operationTablesParts[operationID]
	}
	for _, table := range tables {
		err := operationTablesParts.UpdateOperationTablesParts(table)
		if err != nil {
			return xerrors.Errorf("UpdateOperationTablesParts returned error, err: %w", err)
		}
	}

	return nil
}

func (f *CoordinatorInMemory) CreateOperationWorkers(operationID string, workersCount int) error {
	if workersCount < 2 {
		return xerrors.Errorf("invalid workers count '%v'", workersCount)
	}
	logger.Log.Infof("CreateOperationWorkers operationID: %s, workersCount: %d", operationID, workersCount)
	f.mu.Lock()
	defer f.mu.Unlock()

	arr := make([]*model.OperationWorker, 0)
	for i := 1; i <= workersCount; i++ {
		arr = append(arr, &model.OperationWorker{
			OperationID: operationID,
			WorkerIndex: i,
			Completed:   false,
			Err:         "",
			Progress:    nil,
		})
	}
	f.operationIdToWorkers[operationID] = arr
	return nil
}

func (f *CoordinatorInMemory) GetOperationWorkers(operationID string) ([]*model.OperationWorker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	w := f.operationIdToWorkers[operationID]
	if w == nil {
		return []*model.OperationWorker{}, nil
	}
	return w, nil
}

func (f *CoordinatorInMemory) FinishOperation(operationID, _, _ string, shardIndex int, taskErr error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for index := range f.operationIdToWorkers[operationID] {
		if f.operationIdToWorkers[operationID][index].WorkerIndex == shardIndex {
			f.operationIdToWorkers[operationID][index].Completed = true
			if taskErr != nil {
				f.operationIdToWorkers[operationID][index].Err = taskErr.Error()
			}
			return nil
		}
	}
	return xerrors.New("Operation worker not found")
}

func (f *CoordinatorInMemory) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	workers := f.operationIdToWorkers[operationID]
	n := 0
	for _, w := range workers {
		if w.Completed == completed {
			n++
		}
	}
	return n, nil
}

func (f *CoordinatorInMemory) GetOperationProgress(operationID string) (*model.AggregatedProgress, error) {
	f.mu.Lock()
	otp := f.operationTablesParts[operationID]
	f.mu.Unlock()
	if otp == nil {
		return model.NewAggregatedProgress(), nil
	}
	parts := otp.ListParts()
	ag := model.NewAggregatedProgress()
	ag.PartsCount = int64(len(parts))
	for _, pt := range parts {
		if pt.Completed {
			ag.CompletedPartsCount++
		}
		ag.ETARowsCount += int64(pt.ETARows)
		ag.CompletedRowsCount += int64(pt.CompletedRows)
	}
	return ag, nil
}

func (f *CoordinatorInMemory) OperationHealth(ctx context.Context, operationID string, workerIndex int, workerTime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.operationHealth == nil {
		f.operationHealth = make(map[string]map[int]time.Time)
	}
	if f.operationHealth[operationID] == nil {
		f.operationHealth[operationID] = make(map[int]time.Time)
	}
	f.operationHealth[operationID][workerIndex] = workerTime
	return nil
}
