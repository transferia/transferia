//go:build !disable_yt_provider

package target

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	baseevent "github.com/transferia/transferia/pkg/base/events"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/providers/yt/copy/events"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/pool"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type YtCopyTarget struct {
	cfg        *yt_provider.YtCopyDestination
	yt         yt.Client
	snapshotTX yt.Tx
	pool       pool.Pool
	logger     log.Logger
	metrics    metrics.Registry
	transferID string
}

type ytMimimalClient interface {
	yt.CypressClient
	yt.OperationStartClient
}

type copyTask struct {
	evt      events.TableEvent
	yt       ytMimimalClient
	onFinish func(error)
}

func boolPtr(val bool) *bool {
	return &val
}

func (t *YtCopyTarget) runCopy(task copyTask) error {
	ctx := context.Background()
	tbl := task.evt.Table()

	outPath := t.cfg.Prefix + "/" + tbl.Name
	outYPath, err := ypath.Parse(outPath)
	if err != nil {
		return xerrors.Errorf("error parsing ypath %s: %w", outPath, err)
	}

	copySpec := spec.Spec{
		Title:           fmt.Sprintf("TM RemoteCopy (TransferID %s)", t.transferID),
		ClusterName:     tbl.Cluster,
		InputTablePaths: []ypath.YPath{tbl.OriginalYPath()},
		OutputTablePath: outYPath,
		CopyAttributes:  boolPtr(true),
		Pool:            t.cfg.Pool,
		ResourceLimits:  t.cfg.ResourceLimits,
	}

	if _, err := task.yt.CreateNode(ctx, outYPath, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	}); err != nil {
		return xerrors.Errorf("error creating (if not exists) node %s: %w", outYPath.YPath().String(), err)
	}

	opID, err := task.yt.StartOperation(ctx, yt.OperationRemoteCopy, &copySpec, nil)
	if err != nil {
		return xerrors.Errorf("error starting RemoteCopy from %s.%s to %s.%s: %w",
			copySpec.ClusterName,
			copySpec.InputTablePaths[0].YPath().String(),
			t.cfg.Cluster,
			outPath,
			err)
	}
	for {
		status, err := t.yt.GetOperation(ctx, opID, nil)
		if err != nil {
			return xerrors.Errorf("failed to get RemoteCopy (id=%s) status for table %s: %w", opID, outPath, err)
		}
		if !status.State.IsFinished() {
			time.Sleep(5 * time.Second)
			continue
		}
		if status.State != yt.StateCompleted {
			return xerrors.Errorf("RemoteCopy (id=%s) error for table %s: %w", opID, outPath, status.Result.Error)
		}
		break
	}
	return nil
}

func (t *YtCopyTarget) AsyncPush(in base.EventBatch) chan error {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	t.logger.Debug("Got new EventBatch")
	switch input := in.(type) {
	case *events.EventBatch:
		var ytTxClient ytMimimalClient = t.yt
		if t.cfg.UsePushTransaction {
			tx, err := t.yt.BeginTx(context.Background(), nil)
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to start snapshot TX: %w", err))
			}

			rollbacks.Add(func() {
				if err := tx.Abort(); err != nil {
					t.logger.Error("error commiting push tx", log.Error(err))
				}
			})
			ytTxClient = tx
		}

		var errs util.Errors
		var wg sync.WaitGroup
		onFinish := func(err error) {
			wg.Done()
			if err == nil {
				input.TableProcessed()
			} else {
				errs = util.AppendErr(errs, err)
			}
		}

		for input.Next() {
			rawEvt, err := input.Event()
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("cannot get event from batch: %w", err))
			}
			evt, ok := rawEvt.(events.TableEvent)
			if !ok {
				return util.MakeChanWithError(xerrors.Errorf("unknown event type: %v", evt))
			}

			wg.Add(1)
			t.logger.Debugf("Adding task to copy %s", evt.Table().FullName())
			if err := t.pool.Add(copyTask{evt, ytTxClient, onFinish}); err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to add table %s copy task to the pool: %w", evt.Table().FullName(), err))
			}
		}

		t.logger.Info("Waiting for all table copy task to be done")
		wg.Wait()
		if errs != nil {
			return util.MakeChanWithError(xerrors.Errorf("task error: %w", errs))
		}

		rollbacks.Cancel()
		if t.cfg.UsePushTransaction {
			if err := ytTxClient.(yt.Tx).Commit(); err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to commit snapshot tx: %w", err))
			}
		}
		t.logger.Debug("Done processing EventBatch")
		return util.MakeChanWithError(nil)
	case base.EventBatch:
		for input.Next() {
			ev, err := input.Event()
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to extract event: %w", err))
			}
			switch ev.(type) {
			case baseevent.CleanupEvent:
				t.logger.Infof("cleanup not yet supported for table: %v, skip", ev)
				continue
			case baseevent.TableLoadEvent:
				// not needed for now
			default:
				return util.MakeChanWithError(xerrors.Errorf("unexpected event type: %T", ev))
			}
		}
		return util.MakeChanWithError(nil)
	default:
		return util.MakeChanWithError(xerrors.Errorf("unexpected input type: %T", in))
	}
}

func (t *YtCopyTarget) Close() error {
	err := t.pool.Close()
	t.yt.Stop()
	return err
}

func NewTarget(logger log.Logger, metrics metrics.Registry, cfg *yt_provider.YtCopyDestination, transferID string) (base.EventTarget, error) {
	y, err := ytclient.FromConnParams(cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("error creating ytrpc client: %w", err)
	}
	t := &YtCopyTarget{
		cfg:        cfg,
		yt:         y,
		snapshotTX: nil,
		pool:       nil,
		logger:     logger,
		metrics:    metrics,
		transferID: transferID,
	}
	t.pool = pool.NewDefaultPool(func(in interface{}) {
		task, ok := in.(copyTask)
		if !ok {
			task.onFinish(xerrors.Errorf("unknown task type %T", in))
		}
		task.onFinish(t.runCopy(task))
	}, cfg.Parallelism)
	if err = t.pool.Run(); err != nil {
		return nil, xerrors.Errorf("error starting copy pool: %w", err)
	}

	return t, nil
}
