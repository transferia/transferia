//go:build !disable_postgres_provider

package tasks

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/worker/tasks/cleanup"
)

// CleanupSinker cleans up the sinker when non-incremental transfer is
// activated.
//
// This method changes the sinker database' contents, thus it should be called
// only after the checks on source (that ensure a transfer is possible) are
// completed.
func (l *SnapshotLoader) CleanupSinker(tables abstract.TableMap) error {
	if l.transfer.IncrementOnly() {
		return nil
	}
	if l.transfer.Dst.CleanupMode() == model.DisabledCleanup {
		return nil
	}

	if l.transfer.TmpPolicy != nil {
		if err := model.EnsureTmpPolicySupported(l.transfer.Dst, l.transfer); err != nil {
			return errors.CategorizedErrorf(categories.Target, model.ErrInvalidTmpPolicy, err)
		}
		logger.Log.Info("sink cleanup skipped due to tmp policy")
		return nil
	}

	if dstTmpProvider, ok := l.transfer.Dst.(model.TmpPolicyProvider); ok && dstTmpProvider.EnsureCustomTmpPolicySupported() == nil {
		logger.Log.Info("sink cleanup skipped due to enabled custom tmp policy")
		return nil
	}

	mode := l.transfer.Dst.CleanupMode()

	logger.Log.Infof("need to cleanup (%v) tables", string(mode))
	sink, err := sink.MakeAsyncSink(l.transfer, logger.Log, l.registry, l.cp, middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return errors.CategorizedErrorf(categories.Target, "failed to connect to the target database: %w", err)
	}
	defer sink.Close()

	toCleanupTables := tables.Copy()
	if isPostgresHomoTransfer(l.transfer) && l.transfer.Dst.CleanupMode() == model.Drop {
		viewsOnDst, err := getDestinationDependentViews(l.transfer, tables)
		if err != nil {
			return errors.CategorizedErrorf(categories.Target, "failed to get dependent views from destination database: %w", err)
		}

		viewsOnSrc, err := getSourceDependentViews(l.transfer, tables)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to get dependent views from source database: %w", err)
		}

		if err := checkDependentViewsPostgresHomo(l.transfer, viewsOnSrc, viewsOnDst); err != nil {
			return errors.CategorizedErrorf(categories.Target, "failed dependent VIEWs check: %w", err)
		}
		if len(viewsOnDst) > 0 {
			dependentViews := make([]string, 0)
			tableInfoMock := new(abstract.TableInfo)
			for _, views := range viewsOnDst {
				for _, view := range views {
					if _, ok := toCleanupTables[view]; !ok {
						toCleanupTables[view] = *tableInfoMock
						dependentViews = append(dependentViews, view.Fqtn())
					}
				}
			}
			if len(dependentViews) > 0 {
				logger.Log.Infof("found %d dependent views that will be cleanuped with the original tables: %v", len(dependentViews), dependentViews)
			}
		}
	}

	if err := cleanup.CleanupTables(sink, toCleanupTables, mode); err != nil {
		return errors.CategorizedErrorf(categories.Target, "cleanup (%s) in the target database failed: %w", mode, err)
	}
	return nil
}

func isPostgresHomoTransfer(transfer *model.Transfer) bool {
	if _, ok := transfer.Src.(*postgres.PgSource); !ok {
		return false
	}
	if _, ok := transfer.Dst.(*postgres.PgDestination); !ok {
		return false
	}
	return true
}

func checkDependentViewsPostgresHomo(transfer *model.Transfer, viewsOnSrc, viewsOnDst map[abstract.TableID][]abstract.TableID) error {
	viewsOnDstOnly, viewsOnBoth := leftDiff(viewsOnDst, viewsOnSrc)
	if len(viewsOnDstOnly) > 0 {
		return xerrors.Errorf(
			"there are views on destination database that most likely couldn`t be transferred by transfer because they don`t exist on the source database, that's why we can't delete them automatically, you can do it yourself: %v",
			sprintfDependentViews(viewsOnDstOnly))
	}

	excludedViews := extractExcluded(transfer, viewsOnBoth)
	if len(excludedViews) > 0 {
		return xerrors.Errorf(
			"there are views on destination database that couldn`t be deleted automatically because these views were not included in the transfer, you can delete them yourself or add into include list: %v",
			sprintfDependentViews(excludedViews))
	}

	return nil
}

func extractExcluded(transfer *model.Transfer, dependentViews map[abstract.TableID][]abstract.TableID) map[abstract.TableID][]abstract.TableID {
	excluded := make(map[abstract.TableID][]abstract.TableID)
	src := transfer.Src.(*postgres.PgSource)
	for table, views := range dependentViews {
		excludedViews := make([]abstract.TableID, 0)
		for _, view := range views {
			if !src.Include(view) {
				excludedViews = append(excludedViews, view)
			}
		}
		if len(excludedViews) > 0 {
			excluded[table] = excludedViews
		}
	}
	return excluded
}

func sprintfDependentViews(views map[abstract.TableID][]abstract.TableID) string {
	res := ""
	for table, views := range views {
		if len(res) > 0 {
			res = res + "\n"
		}
		res = res + fmt.Sprintf("%v:", table.Fqtn())
		for _, view := range views {
			res = res + fmt.Sprintf("  %v", view.Fqtn())
		}
	}
	return res
}

func getSourceDependentViews(transfer *model.Transfer, tables abstract.TableMap) (map[abstract.TableID][]abstract.TableID, error) {
	src := transfer.Src.(*postgres.PgSource)
	srcStorage, err := postgres.NewStorage(src.ToStorageParams(transfer))
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to the source database: %w", err)
	}
	defer srcStorage.Close()

	return getDependentViews(srcStorage, tables)
}

func getDestinationDependentViews(transfer *model.Transfer, tables abstract.TableMap) (map[abstract.TableID][]abstract.TableID, error) {
	dst := transfer.Dst.(*postgres.PgDestination)
	dstStorage, err := postgres.NewStorage(dst.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to the destination database: %w", err)
	}
	defer dstStorage.Close()

	return getDependentViews(dstStorage, tables)
}

func getDependentViews(storage *postgres.Storage, tables abstract.TableMap) (map[abstract.TableID][]abstract.TableID, error) {
	ctx := context.Background()
	conn, err := storage.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection to storage: %w", err)
	}
	defer conn.Release()
	pgSchemaExtractor := postgres.NewSchemaExtractor()

	return pgSchemaExtractor.FindDependentViews(ctx, conn.Conn(), tables)
}

func leftDiff(left, right map[abstract.TableID][]abstract.TableID) (leftOnly, intersect map[abstract.TableID][]abstract.TableID) {
	leftOnly = make(map[abstract.TableID][]abstract.TableID)
	intersect = make(map[abstract.TableID][]abstract.TableID)
	for table, lViews := range left {
		if rViews, ok := right[table]; !ok {
			leftOnly[table] = lViews
		} else {
			rViewsSet := make(map[abstract.TableID]bool)
			for _, rView := range rViews {
				rViewsSet[rView] = true
			}
			tableDiff := make([]abstract.TableID, 0)
			tableIntersect := make([]abstract.TableID, 0)
			for _, lView := range lViews {
				if _, ok := rViewsSet[lView]; !ok {
					tableDiff = append(tableDiff, lView)
				} else {
					tableIntersect = append(tableIntersect, lView)
				}
			}
			if len(tableDiff) > 0 {
				leftOnly[table] = tableDiff
			}
			if len(tableIntersect) > 0 {
				intersect[table] = tableIntersect
			}
		}
	}
	return leftOnly, intersect
}
