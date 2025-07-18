package tasks

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type UploadSpec struct {
	Tables []abstract.TableDescription
}

func missingTables(transfer *model.Transfer, registry metrics.Registry, requested []abstract.TableDescription) (result []string, err error) {
	presentTables, err := ObtainAllSrcTables(transfer, registry)
	if err != nil {
		return nil, xerrors.Errorf(TableListErrorText, err)
	}

	for _, rTD := range requested {
		if _, present := presentTables[rTD.ID()]; !present {
			result = append(result, rTD.String())
		}
	}

	return result, nil
}

func inaccessibleTables(transfer *model.Transfer, registry metrics.Registry, requested []abstract.TableDescription) ([]string, error) {
	srcStorage, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), registry)
	if err != nil {
		return nil, xerrors.Errorf(ResolveStorageErrorText, err)
	}
	defer srcStorage.Close()

	sampleableSrcStorage, ok := srcStorage.(abstract.SampleableStorage)
	if !ok {
		return nil, nil
	}

	result := make([]string, 0)
	for _, rTD := range requested {
		if !sampleableSrcStorage.TableAccessible(rTD) {
			result = append(result, rTD.String())
		}
	}

	return result, nil
}

func Upload(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task *model.TransferOperation, spec UploadSpec, registry metrics.Registry) error {
	var taskID string
	if task != nil {
		taskID = task.OperationID
	}
	if transfer.IsTransitional() {
		if transfer.AsyncOperations {
			return xerrors.New("Transitional upload is not supported")
		}
		// there is no code to change, if you need to change it - think twice.
		return TransitUpload(ctx, cp, transfer, task, spec, registry)
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	snapshotLoader := NewSnapshotLoader(cp, taskID, &transfer, registry)
	if !transfer.IsMain() {
		if err := snapshotLoader.UploadTables(ctx, nil, false); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
		return nil
	}

	if !transfer.AsyncOperations {
		rollbacks.Add(func() {
			if err := cp.SetStatus(transfer.ID, model.Failed); err != nil {
				logger.Log.Error("Unable to change status", log.Any("id", transfer.ID), log.Any("task_id", taskID))
			}
		})
		if err := StopJob(cp, transfer); err != nil {
			return xerrors.Errorf("stop job: %w", err)
		}
		if err := cp.SetStatus(transfer.ID, model.Scheduled); err != nil {
			return xerrors.Errorf("unable to set controlplane status: %w", err)
		}
	}

	if transfer.IsAbstract2() {
		if err := snapshotLoader.UploadV2(context.Background(), nil, spec.Tables); err != nil {
			return xerrors.Errorf("upload (v2) failed: %w", err)
		}
	} else {
		if missing, err := missingTables(&transfer, registry, spec.Tables); err != nil {
			return xerrors.Errorf("Failed to check tables' presence in source (%v): %w", transfer.ID, err)
		} else if len(missing) > 0 {
			return xerrors.Errorf("Missing tables in source (%v): %v", transfer.SrcType(), missing)
		}

		if inaccessible, err := inaccessibleTables(&transfer, registry, spec.Tables); err != nil {
			return xerrors.Errorf("Failed to check tables' accessibility in source (%v): %w", transfer.ID, err)
		} else if len(inaccessible) > 0 {
			return xerrors.Errorf("Inaccessible tables (for which data transfer is lacking read privilege) in source (%v): %v", transfer.SrcType(), inaccessible)
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
		if err := snapshotLoader.UploadTables(ctx, spec.Tables, false); err != nil {
			return xerrors.Errorf("Failed to UploadTables (%v): %w", transfer.ID, err)
		}
	}

	if !transfer.AsyncOperations {
		if err := StartJob(ctx, cp, transfer, task); err != nil {
			return xerrors.Errorf("unable to start job: %w", err)
		}
	}

	rollbacks.Cancel()
	return nil
}
