package local

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

type SyncTask struct {
	task     *model.TransferOperation
	logger   log.Logger
	transfer model.Transfer
	wg       *sync.WaitGroup
	cp       coordinator.Coordinator
}

func (s *SyncTask) Stop() {
	s.wg.Wait()
}

func (s *SyncTask) Runtime() abstract.Runtime {
	return new(abstract.LocalRuntime)
}

func (s *SyncTask) run() {
	defer s.wg.Done()
	runnableTaskType, _ := s.task.TaskType.Task.(abstract.RunnableTask)

	err := tasks.Run(
		context.Background(),
		*s.task,
		runnableTaskType,
		s.cp,
		s.transfer,
		s.task.Params,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
	if err := s.cp.FinishOperation(s.task.OperationID, s.task.TaskType.String(), s.transfer.CurrentJobIndex(), err); err != nil {
		s.logger.Error("unable to call finish operation", log.Error(err))
	}
}

// NewSyncTask only used for local debug, can operate properly only on single machine transfer server installation
// with enable `all_in_one_binary` flag
func NewSyncTask(
	task *model.TransferOperation,
	cp coordinator.Coordinator,
	workflow model.OperationWorkflow,
	transfer model.Transfer,
) (*SyncTask, error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	st := &SyncTask{
		task:     task,
		cp:       cp,
		logger:   logger.Log,
		transfer: transfer,
		wg:       wg,
	}

	if task.Status == model.NewTask {
		if err := workflow.OnStart(task); err != nil {
			st.Stop()
			return nil, xerrors.Errorf("unable to start task workflow: %w", err)
		}
		rt, ok := transfer.Runtime.(*abstract.LocalRuntime)
		if ok && rt.WorkersNum() > 1 {
			for i := 1; i <= rt.WorkersNum(); i++ {
				subTr := st.transfer
				subTr.Runtime = &abstract.LocalRuntime{
					Host:       rt.Host,
					CurrentJob: i,
					ShardingUpload: abstract.ShardUploadParams{
						JobCount:     rt.ShardingUpload.JobCount,
						ProcessCount: rt.ShardingUpload.ProcessCount,
					},
				}
				wg.Add(1)
				sst := &SyncTask{
					wg:       wg,
					task:     task,
					cp:       cp,
					logger:   logger.Log,
					transfer: subTr,
				}
				labels := pprof.Labels("dt_job_id", fmt.Sprint(i))
				go pprof.Do(context.Background(), labels, func(ctx context.Context) {
					sst.run()
				})
			}
		}
		labels := pprof.Labels("dt_job_id", "0")
		go pprof.Do(context.Background(), labels, func(ctx context.Context) {
			st.run()
		})
	} else {
		return nil, abstract.NewFatalError(xerrors.New("task already running"))
	}
	return st, nil
}
