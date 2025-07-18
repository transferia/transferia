package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/storage"
)

func checkReuploadAllowed(src model.Source) error {
	if appendOnlySource, ok := src.(model.AppendOnlySource); ok && appendOnlySource.IsAppendOnly() {
		return xerrors.New("Reupload from append only source is not allowed")
	}
	return nil
}

func Reupload(ctx context.Context, cp coordinator.Coordinator, transfer model.Transfer, task model.TransferOperation, registry metrics.Registry) error {
	if transfer.IsTransitional() {
		if transfer.AsyncOperations {
			return xerrors.New("Transitional reupload is not supported")
		}
		// there is no code to change, if you need to change it - think twice.
		return TransitReupload(ctx, cp, transfer, task, registry)
	}

	snapshotLoader := NewSnapshotLoader(cp, task.OperationID, &transfer, registry)
	if !transfer.IsMain() {
		if err := snapshotLoader.UploadTables(ctx, nil, false); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
		return nil
	}
	if err := checkReuploadAllowed(transfer.Src); err != nil {
		return xerrors.Errorf("Reupload is forbidden: %w", err)
	}

	if err := StopJob(cp, transfer); err != nil {
		return xerrors.Errorf("stop job: %w", err)
	}

	if !transfer.IncrementOnly() && !transfer.AsyncOperations {
		err := cp.SetStatus(transfer.ID, model.Started)
		if err != nil {
			return xerrors.Errorf("Cannot update transfer status: %w", err)
		}
	}

	if err := AddExtraTransformers(ctx, &transfer, registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}

	if transfer.IsAbstract2() {
		if err := snapshotLoader.UploadV2(ctx, nil, nil); err != nil {
			return xerrors.Errorf("upload (v2) failed: %w", err)
		}
	} else {
		if transfer.Dst.CleanupMode() != model.DisabledCleanup {
			tables, err := ObtainAllSrcTables(&transfer, registry)
			if err != nil {
				if !xerrors.Is(err, storage.UnsupportedSourceErr) {
					return xerrors.Errorf(TableListErrorText, err)
				}
			}

			if err := snapshotLoader.CleanupSinker(tables); err != nil {
				return xerrors.Errorf("cleanup failed: %w", err)
			}
		}
		if err := snapshotLoader.LoadSnapshot(ctx); err != nil {
			return xerrors.Errorf("Failed to execute LoadSnapshot: %w", err)
		}
	}

	if err := StartJob(ctx, cp, transfer, &task); err != nil {
		return xerrors.Errorf("Failed to start job: %w", err)
	}

	return nil
}
