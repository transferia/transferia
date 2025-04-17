package tasks

import (
	"context"
	"encoding/json"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/data"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var NoTablesError = xerrors.New("Unable to find any tables")

func ActivateDelivery(ctx context.Context, task *model.TransferOperation, cp coordinator.Coordinator, transfer model.Transfer, registry metrics.Registry) error {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() {
		// apparently this may happen only if replication was started ?
		// for async activation we will not need it only for replication stage
		// TODO: remove set status when async replication is implemented
		if !transfer.AsyncOperations || !transfer.SnapshotOnly() {
			if err := cp.SetStatus(transfer.ID, model.Failing); err != nil {
				logger.Log.Warn("failed to set failing transfer's status", log.Error(err))
			}
		}
	})

	var operationID string
	if task != nil {
		operationID = task.OperationID
	}
	snapshotLoader := NewSnapshotLoader(cp, operationID, &transfer, registry)

	if !transfer.IsMain() {
		rollbacks.Cancel()
		logger.Log.Info("ActivateDelivery starts on secondary worker")
		if !transfer.IncrementOnly() {
			if err := snapshotLoader.UploadTables(ctx, nil, true); err != nil {
				return xerrors.Errorf("Snapshot loading failed: %w", err)
			}
		}
		logger.Log.Info("ActivateDelivery finished successfully on secondary worker")
		return nil
	}

	notFirstRun, err := snapshotLoader.OperationStateExists(ctx)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "failed to check existence of operation state: %w", err)
	}
	if notFirstRun {
		return xerrors.New("main worker job was terminated by runtime. Check logs to see the cause")
	}

	logger.Log.Info("ActivateDelivery starts on primary worker")

	if transfer.IsAbstract2() {
		dataProvider, err := data.NewDataProvider(
			logger.Log,
			registry,
			&transfer,
			cp,
		)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "unable to create data provider: %w", err)
		}
		if err := dataProvider.Ping(); err != nil {
			return errors.CategorizedErrorf(categories.Source, "unable to ping data provider: %w", err)
		}
		if err := dataProvider.Init(); err != nil {
			return errors.CategorizedErrorf(categories.Source, "unable to init data provider: %w", err)
		}

		if !transfer.IncrementOnly() && !transfer.AsyncOperations {
			err := cp.SetStatus(transfer.ID, model.Started)
			if err != nil {
				return errors.CategorizedErrorf(categories.Internal, "Cannot update transfer status: %w", err)
			}
		}

		if trackerProvider, ok := dataProvider.(base.TrackerProvider); ok {
			if err := trackerProvider.ResetTracker(transfer.Type); err != nil {
				return errors.CategorizedErrorf(categories.Source, "error reseting tracker: %w", err)
			}
		}

		if !transfer.IncrementOnly() {
			snapshotProvider, ok := dataProvider.(base.SnapshotProvider)
			if !ok {
				return errors.CategorizedErrorf(categories.Source, "source is not SnapshotProvider, so does not support snapshot")
			}
			if err := snapshotLoader.UploadV2(ctx, snapshotProvider, nil); err != nil {
				return xerrors.Errorf("unable to upload (v2): %w", err)
			}
		}
		if err := StartJob(ctx, cp, transfer, task); err != nil {
			return xerrors.Errorf("Cannot start replication: %w", err)
		}

		rollbacks.Cancel()
		logger.Log.Info("ActivateDelivery finished successfully on primary worker")
		return nil
	}

	tables, err := ObtainAllSrcTables(&transfer, registry)
	if !xerrors.Is(err, storage.UnsupportedSourceErr) {
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "Cannot retrieve table information from the source database: %w", err)
		}

		if transfer.SnapshotOnly() {
			if err := coordinator.ReportFakePKey(cp, transfer.ID, coordinator.FakePKeyStatusMessageCategory, nil); err != nil {
				logger.Log.Warn("failed to report fake primary key presence or absence in tables", log.Error(err))
			}
		} else {
			if noKeysTables := tables.NoKeysTables(); len(noKeysTables) > 0 {
				return errors.CategorizedErrorf(categories.Source, "PRIMARY KEY check failed: %v: no key columns found", noKeysTables)
			}
			if err := coordinator.ReportFakePKey(cp, transfer.ID, coordinator.FakePKeyStatusMessageCategory, tables.FakePkeyTables()); err != nil {
				logger.Log.Warn("failed to report fake primary key presence or absence in tables", log.Error(err))
			}
		}
	}

	if err == nil && len(tables) == 0 {
		return NoTablesError
	}

	if !transfer.IncrementOnly() && !transfer.AsyncOperations {
		err := cp.SetStatus(transfer.ID, model.Started)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "Cannot update transfer status: %w", err)
		}
	}

	if err := AddExtraTransformers(ctx, &transfer, registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}

	activator, ok := providers.Source[providers.Activator](logger.Log, registry, cp, &transfer)
	if !ok {
		logger.Log.Infof("no activate hook for: %s", transfer.SrcType())
		if err := snapshotLoader.CleanupSinker(tables); err != nil {
			return xerrors.Errorf("cleanup failed: %w", err)
		}
	} else {
		if err := activator.Activate(ctx, task, tables, providers.ActivateCallbacks{
			Cleanup: func(tables abstract.TableMap) error {
				return snapshotLoader.CleanupSinker(tables)
			},
			Upload: func(tables abstract.TableMap) error {
				return snapshotLoader.UploadTables(ctx, tables.ConvertToTableDescriptions(), true)
			},
			CheckIncludes: func(tables abstract.TableMap) error {
				return snapshotLoader.CheckIncludeDirectives(tables.ConvertToTableDescriptions())
			},
			Rollbacks: &rollbacks,
		}); err != nil {
			return xerrors.Errorf("failed to execute %s activate hook: %w", transfer.SrcType(), err)
		}
	}

	if err := StartJob(ctx, cp, transfer, task); err != nil {
		return xerrors.Errorf("Cannot start replication: %w", err)
	}

	rollbacks.Cancel()
	logger.Log.Info("ActivateDelivery finished successfully on primary worker")
	return nil
}

// ObtainAllSrcTables uses a temporary Storage for transfer source to obtain a list of tables
func ObtainAllSrcTables(transfer *model.Transfer, registry metrics.Registry) (abstract.TableMap, error) {
	srcStorage, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), registry)
	if err != nil {
		return nil, xerrors.Errorf(ResolveStorageErrorText, err)
	}
	defer srcStorage.Close()
	result, err := model.FilteredTableList(srcStorage, transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to list and filter tables in source: %w", err)
	}
	if !transfer.SnapshotOnly() && transfer.SrcType() != transfer.DstType() {
		model.ExcludeViews(result)
	}

	for tableID, tableInfo := range result {
		jsonSchema, _ := json.Marshal(tableInfo.Schema.Columns())
		logger.Log.Info("got table schema", log.String("table", tableID.Fqtn()), log.ByteString("table_schema", jsonSchema))
	}
	return result, nil
}
