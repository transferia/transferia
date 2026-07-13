package target

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	baseevent "github.com/transferia/transferia/pkg/abstract2/events"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_copy_events "github.com/transferia/transferia/pkg/providers/yt/copy/events"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/worker_pool"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type YtCopyTarget struct {
	cfg        *provider_yt.YtCopyDestination
	yt         yt.Client
	snapshotTX yt.Tx
	pool       worker_pool.WorkerPool
	logger     log.Logger
	metrics    core_metrics.Registry
	transferID string
}

type ytMimimalClient interface {
	yt.CypressClient
	yt.OperationStartClient
}

// User attribute on the target node storing source content_revision (TM-9579).
// If source content_revision equals this value, we skip copying the node.
const ContentRevisionAttr = "__dt_content_revision"

type copyTask struct {
	evt      yt_copy_events.NodeEvent
	yt       ytMimimalClient
	onFinish func(error)
}

func boolPtr(val bool) *bool {
	return &val
}

func (t *YtCopyTarget) runCopy(task copyTask) error {
	ctx := context.Background()
	tbl := task.evt.Node()

	outPath := strings.TrimRight(t.cfg.Prefix, "/") + "/" + tbl.Name
	outYPath, err := ypath.Parse(outPath)
	if err != nil {
		return xerrors.Errorf("error parsing ypath %s: %w", outPath, err)
	}

	if t.cfg.SkipUnchangedTables && tbl.ContentRevision != 0 {
		// Skip copy if source content_revision unchanged.
		exists, err := task.yt.NodeExists(ctx, outYPath, nil)
		if err != nil {
			return xerrors.Errorf("error checking target node %s: %w", outPath, err)
		}
		if exists {
			revAttrPath := outYPath.Copy().Child("@" + ContentRevisionAttr)
			attrExists, err := task.yt.NodeExists(ctx, revAttrPath, nil)
			if err != nil {
				return xerrors.Errorf("error checking %s on target %s: %w", ContentRevisionAttr, outPath, err)
			}
			if attrExists {
				var storedRev int64
				err := task.yt.GetNode(ctx, revAttrPath, &storedRev, nil)
				if err == nil && storedRev == tbl.ContentRevision {
					t.logger.Infof("Skipping node %s (source content_revision %d unchanged)", tbl.FullName(), tbl.ContentRevision)
					return nil
				}
			}
		}
	}

	tmpOutYPath := outYPath
	if t.cfg.Cleanup == model.Replace {
		tmpOutYPath, err = ypath.Parse(model.MakeTmpTableName(outPath, t.transferID, model.TmpTableSuffix))
		if err != nil {
			return xerrors.Errorf("error parsing tmp ypath %s: %w", outPath, err)
		}
	}

	copySpec := spec.Spec{
		Title:                    fmt.Sprintf("DataTransfer RemoteCopy (TransferID %s)", t.transferID),
		ClusterName:              tbl.Cluster,
		InputTablePaths:          []ypath.YPath{tbl.OriginalYPath()},
		OutputTablePath:          tmpOutYPath,
		CopyAttributes:           boolPtr(true),
		Pool:                     t.cfg.Pool,
		ResourceLimits:           t.cfg.ResourceLimits,
		AllowUnfrozenInputTables: t.cfg.AllowUnfrozenInputTables,
	}

	createOpts := &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: t.cfg.Cleanup != model.Drop,
		Force:          t.cfg.Cleanup == model.Drop,
	}
	if tbl.Dynamic {
		createOpts.Attributes = map[string]any{
			"dynamic": true,
			"schema":  tbl.Schema,
		}
	}
	if _, err := task.yt.CreateNode(ctx, tmpOutYPath, tbl.NodeType, createOpts); err != nil {
		return xerrors.Errorf("error creating (if not exists) node %s: %w", tmpOutYPath.YPath().String(), err)
	}

	var opID yt.OperationID
	for attempt := 0; ; attempt++ {
		opID, err = task.yt.StartOperation(ctx, yt.OperationRemoteCopy, &copySpec, nil)
		if err == nil {
			break
		}
		if yterrors.ContainsErrorCode(err, yterrors.CodeTooManyOperations) && attempt < 5 {
			backoff := time.Duration(1<<attempt) * time.Second
			t.logger.Warnf("YT pool limit reached, retrying RemoteCopy for %s in %v (attempt %d)", outPath, backoff, attempt+1)
			time.Sleep(backoff)
			continue
		}
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
			return xerrors.Errorf("failed to get RemoteCopy (id=%s) status for node %s: %w", opID, outPath, err)
		}
		if !status.State.IsFinished() {
			time.Sleep(5 * time.Second)
			continue
		}
		if status.State != yt.StateCompleted {
			return xerrors.Errorf("RemoteCopy (id=%s) error for node %s: %w", opID, outPath, status.Result.Error)
		}
		break
	}

	// Move tmp node
	if t.cfg.Cleanup == model.Replace {
		moveOptions := provider_yt.ResolveMoveOptions(task.yt, tmpOutYPath.YPath(), false)

		if _, err := task.yt.MoveNode(
			ctx,
			tmpOutYPath,
			outYPath,
			moveOptions,
		); err != nil {
			return xerrors.Errorf("unable to move tmp node: %w", err)
		}
	}

	if tbl.Dynamic && tbl.PivotKeys != nil {
		t.logger.Infof("Resharding dynamic table %s with pivot_keys %v", outPath, tbl.PivotKeys)
		if err := t.yt.ReshardTable(ctx, outYPath.YPath(), &yt.ReshardTableOptions{PivotKeys: tbl.PivotKeys}); err != nil {
			return xerrors.Errorf("error resharding dynamic table %s: %w", outPath, err)
		}
		t.logger.Infof("Successfully resharded dynamic table %s", outPath)
	}

	if t.cfg.SkipUnchangedTables && tbl.ContentRevision != 0 {
		// Store source content_revision on target so next run can skip if unchanged.
		revAttrPath := outYPath.Copy().Child("@" + ContentRevisionAttr)
		if err := task.yt.SetNode(ctx, revAttrPath, tbl.ContentRevision, nil); err != nil {
			t.logger.Warnf("Failed to set %s on target node %s (next run may re-copy): %v", ContentRevisionAttr, outPath, err)
		}
	}
	return nil
}

func (t *YtCopyTarget) AsyncPush(in abstract2.EventBatch) chan error {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	t.logger.Debug("Got new EventBatch")
	switch input := in.(type) {
	case *yt_copy_events.EventBatch:
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

		var errs []error
		var wg sync.WaitGroup
		onFinish := func(err error) {
			wg.Done()
			if err == nil {
				input.TableProcessed()
			} else if t.cfg.SkipNodeErrors {
				t.logger.Error("[ytcopy-best-effort] copy task failed", log.Error(err))
			} else {
				errs = append(errs, err)
			}
		}

		for input.Next() {
			rawEvt, err := input.Event()
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("cannot get event from batch: %w", err))
			}
			evt, ok := rawEvt.(yt_copy_events.NodeEvent)
			if !ok {
				return util.MakeChanWithError(xerrors.Errorf("unknown event type: %v", evt))
			}

			wg.Add(1)
			t.logger.Debugf("Adding task to copy %s", evt.Node().FullName())
			if err := t.pool.Add(copyTask{evt, ytTxClient, onFinish}); err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to add %s copy task to the pool: %w", evt.Node().FullName(), err))
			}
		}

		t.logger.Info("Waiting for all copy tasks to be done")
		wg.Wait()
		if !t.cfg.SkipNodeErrors {
			if err := errors.Join(errs...); err != nil {
				return util.MakeChanWithError(xerrors.Errorf("task error: %w", err))
			}
		}

		rollbacks.Cancel()
		if t.cfg.UsePushTransaction {
			if err := ytTxClient.(yt.Tx).Commit(); err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to commit snapshot tx: %w", err))
			}
		}
		t.logger.Debug("Done processing EventBatch")
		return util.MakeChanWithError(nil)
	case abstract2.EventBatch:
		for input.Next() {
			ev, err := input.Event()
			if err != nil {
				return util.MakeChanWithError(xerrors.Errorf("unable to extract event: %w", err))
			}
			switch ev.(type) {
			case baseevent.CleanupEvent:
				t.logger.Infof("cleanup not yet supported for node: %v, skip", ev)
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

func NewTarget(logger log.Logger, metrics core_metrics.Registry, cfg *provider_yt.YtCopyDestination, transferID string) (abstract2.EventTarget, error) {
	y, err := yt_client.FromConnParams(cfg, logger)
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
	t.pool = worker_pool.NewDefaultWorkerPool(func(in interface{}) {
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
