package helpers

import (
	"context"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"golang.org/x/sync/errgroup"
)

func defaultShardedActivateTask(tr *model.Transfer) *model.TransferOperation {
	return &model.TransferOperation{
		OperationID: tr.ID,
		TransferID:  tr.ID,
		TaskType:    abstract.TaskType{Task: abstract.Activate{}},
		Status:      model.RunningTask,
		CreatedAt:   time.Now().UTC(),
	}
}

func ActivateShardedWithCP(
	ctx context.Context,
	cp coordinator.Coordinator,
	task *model.TransferOperation,
	base *model.Transfer,
	registry core_metrics.Registry,
) (*Worker, error) {
	return activateShardedWithCP(ctx, cp, task, base, registry)
}

func activateShardedWithCP(
	ctx context.Context,
	cp coordinator.Coordinator,
	task *model.TransferOperation,
	base *model.Transfer,
	registry core_metrics.Registry,
) (*Worker, error) {
	if task == nil {
		task = defaultShardedActivateTask(base)
	}
	rt, ok := base.Runtime.(abstract.ShardingTaskRuntime)
	if !ok {
		return nil, xerrors.New("sharded snapshot: transfer.Runtime is not ShardingTaskRuntime")
	}
	jobCount := rt.SnapshotWorkersNum()
	processCount := rt.SnapshotThreadsNumPerWorker()
	if jobCount <= 1 {
		return nil, xerrors.New("sharded snapshot: SnapshotWorkersNum must be > 1")
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		tr := *base
		tr.Runtime = &abstract.LocalRuntime{
			CurrentJob:     0,
			ShardingUpload: abstract.ShardUploadParams{JobCount: jobCount, ProcessCount: processCount},
		}
		return tasks.ActivateDelivery(gCtx, task, cp, tr, registry)
	})

	for j := 1; j <= jobCount; j++ {
		j := j
		g.Go(func() error {
			tr := *base
			tr.Runtime = &abstract.LocalRuntime{
				CurrentJob:     j,
				ShardingUpload: abstract.ShardUploadParams{JobCount: jobCount, ProcessCount: processCount},
			}
			err := tasks.ActivateDelivery(gCtx, task, cp, tr, registry)
			// Always report to coordinator so main worker can detect secondary failures.
			_ = cp.FinishOperation(task.OperationID, "", "", j, err)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	result := &Worker{cp: cp}
	if base.Type == abstract.TransferTypeSnapshotAndIncrement || base.Type == abstract.TransferTypeIncrementOnly {
		result.initLocalWorker(base)
		result.worker.Start()
	}
	return result, nil
}
