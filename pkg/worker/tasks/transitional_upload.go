package tasks

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/terryid"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// TransitUpload is shitty method mainly for transfers with LB in the middle,
// so we could make @lupach happy and isolate crappy code in separate func.
func TransitUpload(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task *model.TransferOperation, spec UploadSpec, registry metrics.Registry) error {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	snapshotLoader := NewSnapshotLoader(cp, task.OperationID, &transfer, registry)
	if !transfer.IsMain() {
		if err := snapshotLoader.UploadTables(ctx, nil, false); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
		return nil
	}
	if transfer.IsSharded() && transfer.IsTransitional() {
		return xerrors.New("sharded upload on connected transfers not supported")
	}

	sources, err := GetLeftTerminalSrcEndpoints(cp, transfer)
	if err != nil {
		return xerrors.Errorf("unable to get left terminal source endpoints: %w", err)
	}
	if len(sources) == 0 {
		return xerrors.New("Upload supports maximum one-lb-in-the-middle case")
	}
	rollbacks.Add(func() {
		if err := cp.SetStatus(transfer.ID, model.Failed); err != nil {
			logger.Log.Error("Unable to change status", log.Any("id", transfer.ID), log.Any("task_id", task.OperationID))
		}
	})
	if err := StopJob(cp, transfer); err != nil {
		return xerrors.Errorf("stop job: %w", err)
	}
	if err := cp.SetStatus(transfer.ID, model.Scheduled); err != nil {
		return xerrors.Errorf("unable to set controlplane status: %w", err)
	}

	destinations, err := GetRightTerminalDstEndpoints(cp, transfer)
	if err != nil {
		return xerrors.Errorf("unable to get right terminal destination endpoints: %w", err)
	}

	var transfers []model.Transfer
	for _, src := range sources {
		for _, dst := range destinations {
			utTransfer := model.Transfer{
				ID:                transfer.ID,
				TransferName:      "",
				Description:       "",
				Labels:            "",
				Author:            "",
				Status:            model.Running,
				Type:              abstract.TransferTypeSnapshotOnly,
				FolderID:          "",
				Runtime:           transfer.Runtime,
				Src:               src,
				Dst:               dst,
				CloudID:           "",
				RegularSnapshot:   nil,
				Transformation:    transfer.Transformation,
				TmpPolicy:         transfer.TmpPolicy,
				DataObjects:       transfer.DataObjects,
				TypeSystemVersion: transfer.TypeSystemVersion,
				AsyncOperations:   transfer.AsyncOperations,
			}

			if !transfer.IsAbstract2() {
				if missing, err := missingTables(&utTransfer, registry, spec.Tables); err != nil {
					return xerrors.Errorf("Failed to check tables' presence in source (%v): %w", utTransfer.ID, err)
				} else if len(missing) > 0 {
					return xerrors.Errorf("Missing tables in source: %v", missing)
				}

				if inaccessible, err := inaccessibleTables(&utTransfer, registry, spec.Tables); err != nil {
					return xerrors.Errorf("Failed to check tables' accessibility in source (%v): %w", utTransfer.Author, err)
				} else if len(inaccessible) > 0 {
					return xerrors.Errorf("Inaccessible tables (for which data transfer is lacking read privilege) in source: %v", inaccessible)
				}
			}

			transfers = append(transfers, utTransfer)
		}
	}
	if transfer.IsSharded() && len(transfers) > 1 {
		return xerrors.New("sharded upload on connected transfers not supported")
	}

	for _, utTransfer := range transfers {
		logger.Log.Info(utTransfer.ID)

		if transfer.IsAbstract2() {
			if err := snapshotLoader.UploadV2(context.Background(), nil, spec.Tables); err != nil {
				return xerrors.Errorf("upload (v2) failed: %w", err)
			}
			continue
		}

		cleanupTableMap := map[abstract.TableID]abstract.TableInfo{}
		for _, t := range spec.Tables {
			if t.Filter == "" && t.Offset == 0 {
				cleanupTableMap[t.ID()] = abstract.TableInfo{EtaRow: t.EtaRow, IsView: false, Schema: nil}
			}
		}
		if err := snapshotLoader.CleanupSinker(cleanupTableMap); err != nil {
			return xerrors.Errorf("Failed to clean up pusher: %w", err)
		}
		if len(transfers) == 1 && !transfer.IsTransitional() {
			utTransfer = transfer // we should save original transfer object if there is no transit upload pairs
		}

		utSnapshotLoader := NewSnapshotLoader(cp, task.OperationID, &utTransfer, registry)
		if err := utSnapshotLoader.UploadTables(ctx, spec.Tables, false); err != nil {
			return xerrors.Errorf("Failed to UploadTables (%v): %w", utTransfer.ID, err)
		}
	}

	if err := StartJob(ctx, cp, transfer, task); err != nil {
		return xerrors.Errorf("unable to start job: %w", err)
	}
	rollbacks.Cancel()
	return nil
}

// TransitReupload is shitty method mainly for transfers with LB in the middle, same as TransitUpload
func TransitReupload(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, registry metrics.Registry) error {
	snapshotLoader := NewSnapshotLoader(cp, task.OperationID, &transfer, registry)
	if !transfer.IsMain() {
		if err := snapshotLoader.UploadTables(ctx, nil, false); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
		return nil
	}
	srcs, err := GetLeftTerminalSrcEndpoints(cp, transfer)
	if err != nil {
		return xerrors.Errorf("unable to get left terminal source endpoints: %w", err)
	}
	if len(srcs) == 0 {
		return xerrors.New("Reupload supports maximum one-lb-in-the-middle case")
	}
	dsts, err := GetRightTerminalDstEndpoints(cp, transfer)
	if err != nil {
		return xerrors.Errorf("unable to get right terminal destination endpoints: %w", err)
	}

	var transfers []*model.Transfer

	for _, src := range srcs {
		for _, dst := range dsts {
			if err := checkReuploadAllowed(src); err != nil {
				return xerrors.Errorf("Reupload is forbidden: %w", err)
			}

			rut := new(model.Transfer)
			rut.Src = src
			rut.Dst = dst
			rut.ID = transfer.ID
			rut.Transformation = transfer.Transformation
			transfers = append(transfers, rut)
		}
	}

	if err := StopJob(cp, transfer); err != nil {
		return xerrors.Errorf("stop job: %w", err)
	}

	if !transfer.IncrementOnly() {
		err := cp.SetStatus(transfer.ID, model.Started)
		if err != nil {
			return xerrors.Errorf("Cannot update transfer status: %w", err)
		}
	}

	for _, rut := range transfers {
		rutSnapshotLoader := NewSnapshotLoader(cp, task.OperationID, rut, registry)
		if rut.IsAbstract2() {
			if err := rutSnapshotLoader.UploadV2(ctx, nil, nil); err != nil {
				return xerrors.Errorf("upload (v2) failed: %w", err)
			}
			continue
		}

		if rut.Dst.CleanupMode() != model.DisabledCleanup {
			tables, err := ObtainAllSrcTables(rut, registry)
			if err != nil {
				if xerrors.Is(err, storage.UnsupportedSourceErr) {
					continue
				}
				return xerrors.Errorf(TableListErrorText, err)
			}

			if err := rutSnapshotLoader.CleanupSinker(tables); err != nil {
				return xerrors.Errorf("cleanup failed: %w", err)
			}
		}
		logger.Log.Infof("Direct upload %v >> %v", transfer.SrcType(), transfer.DstType())
		if err := rutSnapshotLoader.LoadSnapshot(ctx); err != nil {
			return xerrors.Errorf("Failed to execute LoadSnapshot: %w", err)
		}
	}

	if err := StartJob(ctx, cp, transfer, &task); err != nil {
		return xerrors.Errorf("Failed to start job: %w", err)
	}

	return nil
}

// TransitionalAddTables same as above
func TransitionalAddTables(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, tables []string, registry metrics.Registry) error {
	linkedTransfers, err := GetLeftTerminalTransfers(cp, transfer)
	if err != nil {
		return xerrors.Errorf("Unable to resolve left terminal transfer: %w", err)
	}
	if len(linkedTransfers) == 0 {
		return xerrors.New("AddTables supports maximum one-lb-in-the-middle case")
	}
	if err := StopJob(cp, transfer); err != nil {
		return xerrors.Errorf("stop job: %w", err)
	}
	dsts, err := GetRightTerminalDstEndpoints(cp, transfer)
	if err != nil {
		return xerrors.Errorf("Unable to resolve right terminal transfer: %w", err)
	}
	filteredTransfers := make([]model.Transfer, 0)
	for i, tr := range linkedTransfers {
		if !isAllowedSourceType(tr.Src) {
			logger.Log.Errorf("Add tables supported only for pg sources")
			continue
		}

		if err := verifyCanAddTables(tr.Src, tables, &transfer); err != nil {
			logger.Log.Errorf("Unable to add tables: %v", err)
			if i == len(linkedTransfers)-1 {
				return xerrors.Errorf("Cannot verify add tables: %w", err)
			}
		} else {
			filteredTransfers = append(filteredTransfers, *tr)
		}
	}

	if len(filteredTransfers) == 0 {
		return xerrors.New("Unable to find active transfers")
	}

	for _, currTransfer := range filteredTransfers {
		oldTables := replaceSourceTables(currTransfer.Src, tables)
		commonTableSet := make(map[string]bool)
		for _, table := range oldTables {
			commonTableSet[table] = true
		}
		for _, table := range tables {
			commonTableSet[table] = true
		}

		logger.Log.Info("Initial load for tables", log.Any("tables", tables), log.Any("source", currTransfer.SrcType()))
		for _, dst := range dsts {
			syntheticTransfer := currTransfer.Copy(fmt.Sprintf("synthetic_transfer_%v_to_%v_%v",
				currTransfer.SrcType(), currTransfer.DstType(), terryid.GenerateTransferID()))
			syntheticTransfer.Dst = dst
			logger.Log.Infof("Direct upload %v >> %v", syntheticTransfer.SrcType(), syntheticTransfer.DstType())

			syntheticTransfer.FillDependentFields()
			if err := applyAddedTablesSchema(&syntheticTransfer, registry); err != nil {
				return xerrors.Errorf("Cannot load schema for added table to target: %w", err)
			}

			childTask := task.OperationID + "-" + terryid.GenerateJobID()
			snapshotLoader := NewSnapshotLoader(cp, childTask, &syntheticTransfer, registry)
			if err := snapshotLoader.LoadSnapshot(context.TODO()); err != nil {
				return xerrors.Errorf("Unable to load snapshot: %w", err)
			}
		}
		logger.Log.Info("Load done, store added tables in source endpoint and start transfer", log.Any("tables", tables), log.Any("id", currTransfer.ID))

		e, err := cp.GetEndpoint(currTransfer.ID, true)
		if err != nil {
			return xerrors.Errorf("Cannot load source endpoint for updating: %w", err)
		}
		newSrc, _ := e.(model.Source)
		setSourceTables(newSrc, commonTableSet)
		if err := cp.UpdateEndpoint(currTransfer.ID, e); err != nil {
			return xerrors.Errorf("Cannot store source endpoint with added tables: %w", err)
		}
	}
	return StartJob(ctx, cp, transfer, &task)
}
