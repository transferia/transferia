package coordinator

import (
	"sync"

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
	}
}

func (f *CoordinatorInMemory) Progress() []*abstract.OperationTablePart {
	return f.progress
}

func (f *CoordinatorInMemory) GetTransferState(transferID string) (map[string]*TransferStateData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Log.Info("CoordinatorInMemory.SetTransferState", log.Any("transfer_id", transferID))

	return f.state[transferID], nil
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
	return operationTablesParts.AssignOperationTablePart(), nil
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
	f.mu.Lock()
	defer f.mu.Unlock()

	arr := make([]*model.OperationWorker, 0)
	for i := 0; i < workersCount; i++ {
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

	return f.operationIdToWorkers[operationID], nil
}

func (f *CoordinatorInMemory) FinishOperation(operationID, _ string, shardIndex int, _ error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for index := range f.operationIdToWorkers[operationID] {
		if f.operationIdToWorkers[operationID][index].WorkerIndex == shardIndex {
			f.operationIdToWorkers[operationID][index].Completed = true
			return nil
		}
	}
	return xerrors.New("Operation worker not found")
}

func (f *CoordinatorInMemory) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.operationIdToWorkers[operationID]), nil
}

func (f *CoordinatorInMemory) GetOperationProgress(operationID string) (*model.AggregatedProgress, error) {
	return model.NewAggregatedProgress(), nil
}
