package helpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"go.uber.org/zap/zapcore"
)

//---------------------------------------------------------------------------------------------------------------------
// fake cp

type fakeCpErrRepl struct {
	coordinator.Coordinator
	onErrorCallback []func(err error)
}

func (f *fakeCpErrRepl) FailReplication(transferID string, err error) error {
	for _, cb := range f.onErrorCallback {
		cb(err)
	}
	return nil
}

func NewFakeCPErrRepl(onErrorCallback ...func(err error)) coordinator.Coordinator {
	return &fakeCpErrRepl{Coordinator: coordinator.NewStatefulFakeClient(), onErrorCallback: onErrorCallback}
}

//---------------------------------------------------------------------------------------------------------------------
// worker

type Worker struct {
	worker *local.LocalWorker
	cp     coordinator.Coordinator
}

func (w *Worker) initLocalWorker(transfer *model.Transfer) {
	w.worker = local.NewLocalWorker(w.cp, transfer, EmptyRegistry(), logger.LoggerWithLevel(zapcore.DebugLevel))
}

func (w *Worker) Run() error {
	return w.worker.Run()
}

// controlplane that catches replication failure
func (w *Worker) Close(t *testing.T) {
	if w.worker != nil {
		err := w.worker.Stop()
		if xerrors.Is(err, context.Canceled) {
			return
		}
		require.NoError(t, err)
	}
}

func (w *Worker) CloseWithErr() error {
	if w.worker != nil {
		err := w.worker.Stop()
		if xerrors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

// Restart replication worker with updated transfer
func (w *Worker) Restart(t *testing.T, transfer *model.Transfer) {
	w.Close(t)
	w.initLocalWorker(transfer)
	w.worker.Start()
}

//---------------------------------------------------------------------------------------------------------------------
// functions

func Activate(t *testing.T, transfer *model.Transfer, onErrorCallback ...func(err error)) *Worker {
	return activate(t, transfer, true, onErrorCallback...)
}

func ActivateWithoutStart(t *testing.T, transfer *model.Transfer, onErrorCallback ...func(err error)) *Worker {
	return activate(t, transfer, false, onErrorCallback...)
}

func activate(t *testing.T, transfer *model.Transfer, isStart bool, onErrorCallback ...func(err error)) *Worker {
	if len(onErrorCallback) == 0 {
		// append default callback checker: no error!
		onErrorCallback = append(onErrorCallback, func(err error) {
			require.NoError(t, err)
		})
	}
	result, err := activateErr(transfer, isStart, onErrorCallback...)
	require.NoError(t, err)
	return result
}

func ActivateErr(transfer *model.Transfer, onErrorCallback ...func(err error)) (*Worker, error) {
	return activateErr(transfer, true, onErrorCallback...)
}

func activateErr(transfer *model.Transfer, isStart bool, onErrorCallback ...func(err error)) (*Worker, error) {
	cp := NewFakeCPErrRepl(onErrorCallback...)
	return ActivateWithCP(transfer, cp, isStart)
}

func ActivateWithCP(transfer *model.Transfer, cp coordinator.Coordinator, isStart bool) (*Worker, error) {
	result := &Worker{
		worker: nil,
		cp:     cp,
	}

	err := tasks.ActivateDelivery(context.Background(), nil, result.cp, *transfer, EmptyRegistry())
	if err != nil {
		return nil, err
	}

	if transfer.Type == abstract.TransferTypeSnapshotAndIncrement || transfer.Type == abstract.TransferTypeIncrementOnly {
		result.initLocalWorker(transfer)
		if isStart {
			result.worker.Start()
		}
	}

	return result, nil
}
