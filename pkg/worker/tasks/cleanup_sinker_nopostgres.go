//go:build disable_postgres_provider

package tasks

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
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

	if err := cleanup.CleanupTables(sink, toCleanupTables, mode); err != nil {
		return errors.CategorizedErrorf(categories.Target, "cleanup (%s) in the target database failed: %w", mode, err)
	}
	return nil
}
