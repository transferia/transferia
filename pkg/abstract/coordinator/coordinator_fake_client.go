package coordinator

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type CoordinatorNoOp struct {
	getTransferState func(id string) (map[string]*TransferStateData, error)
}

var _ Coordinator = (*CoordinatorNoOp)(nil)

func NewFakeClient() *CoordinatorNoOp {
	return &CoordinatorNoOp{
		getTransferState: nil,
	}
}

func NewFakeClientWithOpts(
	getTransferState func(id string) (map[string]*TransferStateData, error),
) *CoordinatorNoOp {
	return &CoordinatorNoOp{
		getTransferState: getTransferState,
	}
}

func (f *CoordinatorNoOp) GetTransferState(id string) (map[string]*TransferStateData, error) {
	if f.getTransferState != nil {
		return f.getTransferState(id)
	}
	return nil, nil
}

func (f *CoordinatorNoOp) SetTransferState(transferID string, state map[string]*TransferStateData) error {
	return nil
}

func (f *CoordinatorNoOp) RemoveTransferState(transferID string, stateKeys []string) error {
	return nil
}

func (f *CoordinatorNoOp) SetOperationState(taskID string, state string) error {
	return nil
}

func (f *CoordinatorNoOp) GetOperationState(taskID string) (string, error) {
	return "", OperationStateNotFoundError
}

func (f *CoordinatorNoOp) UploadTable(transferID string, tables []abstract.TableDescription) error {
	return nil
}

func (f *CoordinatorNoOp) FinishOperation(taskID string, taskType string, shardIndex int, taskErr error) error {
	return nil
}

func (f *CoordinatorNoOp) OpenStatusMessage(transferID string, category string, content *StatusMessage) error {
	return nil
}

func (f *CoordinatorNoOp) CloseStatusMessagesForCategory(transferID string, category string) error {
	return nil
}

func (f *CoordinatorNoOp) CloseStatusMessagesForTransfer(transferID string) error {
	return nil
}

func (f *CoordinatorNoOp) FailReplication(transferID string, err error) error {
	return nil
}

func (f *CoordinatorNoOp) TransferHealth(ctx context.Context, transferID string, health *TransferHeartbeat) error {
	return nil
}

func (f *CoordinatorNoOp) OperationHealth(ctx context.Context, operationID string, workerIndex int, workerTime time.Time) error {
	return nil
}

func (f *CoordinatorNoOp) UpdateTestResults(id string, request *abstract.TestResult) error {
	return nil
}

func (f *CoordinatorNoOp) GetTransfers(statuses []model.TransferStatus, transferID string) ([]*model.Transfer, error) {
	return nil, nil
}

func (f *CoordinatorNoOp) SetStatus(transferID string, status model.TransferStatus) error {
	logger.Log.Infof("fake change status: %v -> %v", transferID, status)
	return nil
}

func (f *CoordinatorNoOp) GetEndpoint(transferID string, isSource bool) (model.EndpointParams, error) {
	logger.Log.Infof("fake get endpoint: %v", transferID)
	return nil, nil
}

func (f *CoordinatorNoOp) GetEndpointTransfers(transferID string, isSource bool) ([]*model.Transfer, error) {
	logger.Log.Infof("fake get endpoint transfers: %v", transferID)
	return nil, nil
}

func (f *CoordinatorNoOp) UpdateEndpoint(transferID string, endpoint model.EndpointParams) error {
	return nil
}

func (f *CoordinatorNoOp) GetOperationProgress(operationID string) (*model.AggregatedProgress, error) {
	return nil, nil
}

func (f *CoordinatorNoOp) CreateOperationWorkers(operationID string, workersCount int) error {
	return nil
}

func (f *CoordinatorNoOp) GetOperationWorkers(operationID string) ([]*model.OperationWorker, error) {
	return nil, nil
}

func (f *CoordinatorNoOp) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	return 2, nil
}

func (f *CoordinatorNoOp) CreateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	return nil
}

func (f *CoordinatorNoOp) GetOperationTablesParts(operationID string) ([]*model.OperationTablePart, error) {
	return nil, nil
}

func (f *CoordinatorNoOp) AssignOperationTablePart(operationID string, workerIndex int) (*model.OperationTablePart, error) {
	return nil, nil
}

func (f *CoordinatorNoOp) ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error) {
	return 0, nil
}

func (f *CoordinatorNoOp) UpdateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	return nil
}
