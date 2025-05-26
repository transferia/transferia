package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/dataplane/provideradapter"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type runTaskVisitor struct {
	params   interface{}
	task     model.TransferOperation
	cp       coordinator.Coordinator
	transfer model.Transfer
	registry metrics.Registry
	ctx      context.Context
}

func (v runTaskVisitor) OnTestEndpoint(t abstract.TestEndpoint) interface{} {
	var params *TestEndpointParams
	if err := util.MapFromJSON(v.params, &params); err != nil {
		return err
	}
	// for slow endpoints let it be at least 30 minutes
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	tr := abstract.NewTestResult()
	ticker := time.NewTicker(time.Second * 5)
	resCh := make(chan error)
	go func() {
		res := TestEndpoint(v.ctx, params, tr)
		if err := v.cp.UpdateTestResults(v.task.OperationID, res); err != nil {
			logger.Log.Warn("unable to UpdateOtherOperation", log.Error(err))
			resCh <- err
		}
		resCh <- nil
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// refresh test results to CP
				if err := v.cp.UpdateTestResults(v.task.OperationID, tr); err != nil {
					logger.Log.Warn("unable to update test result", log.Error(err))
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return xerrors.New("timeout reached")
	case err := <-resCh:
		return err
	}
}

func (v runTaskVisitor) OnActivate(t abstract.Activate) interface{} {
	if err := ActivateDelivery(v.ctx, &v.task, v.cp, v.transfer, v.registry); err != nil {
		logger.Log.Error("Unable to Activate", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnCleanupResource(t abstract.CleanupResource) interface{} {
	if err := CleanupResource(v.ctx, v.task, v.transfer, logger.Log, v.cp); err != nil {
		logger.Log.Error("Unable to CleanupResource", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnAddTables(t abstract.AddTables) interface{} {
	var tables []string
	if err := util.MapFromJSON(v.params, &tables); err != nil {
		return err
	}
	if err := AddTables(v.ctx, v.cp, v.transfer, v.task, tables, v.registry); err != nil {
		logger.Log.Error("AddTables", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnUpdateTransfer(t abstract.UpdateTransfer) interface{} {
	objects, ok := v.params.(abstract.UpdateTransferParams)
	if !ok {
		return xerrors.Errorf("unexpected params type: %T", v.params)
	}
	if err := UpdateTransfer(v.ctx, v.cp, v.transfer, v.task, v.registry, objects); err != nil {
		logger.Log.Error("UpdateDataobject", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnChecksum(t abstract.Checksum) interface{} {
	var us ChecksumParameters
	if err := util.MapFromJSON(v.params, &us); err != nil {
		return err
	}
	if err := Checksum(v.transfer, logger.Log, v.registry, &us); err != nil {
		return err
	}
	return nil
}

func (v runTaskVisitor) OnDeactivate(t abstract.Deactivate) interface{} {
	if err := Deactivate(v.ctx, v.cp, v.transfer, v.task, v.registry); err != nil {
		logger.Log.Error("Unable to Deactivate", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnReUpload(t abstract.ReUpload) interface{} {
	if err := Reupload(v.ctx, v.cp, v.transfer, v.task, v.registry); err != nil {
		logger.Log.Error("Unable to ReUpload", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnRemoveTables(t abstract.RemoveTables) interface{} {
	var tables []string
	if err := util.MapFromJSON(v.params, &tables); err != nil {
		return err
	}
	if err := RemoveTables(v.ctx, v.cp, v.transfer, v.task, tables); err != nil {
		logger.Log.Error("RemoveTable", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnRestart(t abstract.Restart) interface{} {
	if err := StopJob(v.cp, v.transfer); err != nil {
		return err
	}
	if err := StartJob(v.ctx, v.cp, v.transfer, &v.task); err != nil {
		return err
	}
	return nil
}

func (v runTaskVisitor) OnStart(t abstract.Start) interface{} {
	if err := StartJob(v.ctx, v.cp, v.transfer, &v.task); err != nil {
		logger.Log.Error("Unable to Start", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnStop(t abstract.Stop) interface{} {
	if err := StopJob(v.cp, v.transfer); err != nil {
		logger.Log.Error("Unable to Stop", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnUpload(t abstract.Upload) interface{} {
	var us UploadSpec
	if err := util.MapFromJSON(v.params, &us); err != nil {
		return err
	}
	if err := Upload(v.ctx, v.cp, v.transfer, &v.task, us, v.registry); err != nil {
		logger.Log.Error("Upload", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) OnVerify(t abstract.Verify) interface{} {
	if err := VerifyDelivery(v.transfer, logger.Log, v.registry); err != nil {
		logger.Log.Error("VerifyDelivery", log.Error(err))
		return err
	}
	return nil
}

func (v runTaskVisitor) PushOperationHealthMeta(t time.Time) {
	if err := v.cp.OperationHealth(v.ctx, v.task.OperationID, v.transfer.CurrentJobIndex(), t); err != nil {
		logger.Log.Error("unable send operation health", log.Error(err))
	} else {
		logger.Log.Debug("operation health meta pushed", log.Any("time", t))
	}
}

func Run(ctx context.Context, task model.TransferOperation, command abstract.RunnableTask, cp coordinator.Coordinator, transfer model.Transfer, params interface{}, registry metrics.Registry) error {
	if _, ok := command.(abstract.ShardableTask); !ok {
		if rt, ok := transfer.Runtime.(abstract.ShardingTaskRuntime); ok && !rt.SnapshotIsMain() {
			logger.Log.Warn("run non sharding task inside sharding runtime secondary worker, will do nothing")
			return nil
		}
	}
	if err := provideradapter.ApplyForTransfer(&transfer); err != nil {
		return xerrors.Errorf("unable to adapt transfer: %w", err)
	}

	status := registry.Gauge("task.status")
	status.Set(float64(1))
	defer status.Set(float64(0))

	visitor := runTaskVisitor{
		params:   params,
		task:     task,
		cp:       cp,
		transfer: transfer,
		registry: registry,
		ctx:      ctx,
	}
	done := make(chan struct{})
	ticker := time.NewTicker(time.Second * 15)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				visitor.PushOperationHealthMeta(time.Now().UTC())
				return
			case t := <-ticker.C:
				visitor.PushOperationHealthMeta(t.UTC())
			}
		}
	}()
	defer func() {
		close(done)
		wg.Wait()
	}()
	logger.Log.Infof("Transfer %v -> %v (%v)", transfer.SrcType(), transfer.DstType(), transfer.ID)

	commandName := fmt.Sprintf("%T", command)
	err, _ := command.VisitRunnable(visitor).(error)
	if err != nil {
		logger.Log.Error("Task failed", log.String("task", commandName), log.Error(err))
	} else {
		logger.Log.Info("Task finished", log.String("task", commandName))
	}
	return err
}
