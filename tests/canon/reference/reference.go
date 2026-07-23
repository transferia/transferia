package reference

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/middlewares"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func constructSinkCleanupAndPush(t *testing.T, transfer *model.Transfer, tables abstract.TableMap, itemBatches ...[]abstract.ChangeItem) {
	start := time.Now()
	logger.Log.Debugf("constructSinkCleanupAndPush START tables=%d batches=%d", len(tables), len(itemBatches))

	cp := coordinator.NewStatefulFakeClient()
	logger.Log.Debugf("constructSinkCleanupAndPush: MakeAsyncSink START elapsed=%v", time.Since(start))
	as, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)
	logger.Log.Debugf("constructSinkCleanupAndPush: MakeAsyncSink DONE elapsed=%v", time.Since(start))
	defer func() {
		logger.Log.Debugf("constructSinkCleanupAndPush: AsyncSink.Close START elapsed=%v", time.Since(start))
		require.NoError(t, as.Close())
		logger.Log.Debugf("constructSinkCleanupAndPush: AsyncSink.Close DONE elapsed=%v", time.Since(start))
	}()

	snapshotLoader := tasks.NewSnapshotLoader(cp, &model.TransferOperation{}, transfer, helpers.EmptyRegistry())
	logger.Log.Debugf("constructSinkCleanupAndPush: CleanupSinker START tables=%d elapsed=%v", len(tables), time.Since(start))
	require.NoError(t, snapshotLoader.CleanupSinker(tables))
	logger.Log.Debugf("constructSinkCleanupAndPush: CleanupSinker DONE elapsed=%v", time.Since(start))

	for i := range itemBatches {
		logger.Log.Debugf("constructSinkCleanupAndPush: AsyncPush batch %d/%d START items=%d elapsed=%v", i+1, len(itemBatches), len(itemBatches[i]), time.Since(start))
		errCh := as.AsyncPush(itemBatches[i])
		require.NoError(t, <-errCh)
		logger.Log.Debugf("constructSinkCleanupAndPush: AsyncPush batch %d/%d DONE elapsed=%v", i+1, len(itemBatches), time.Since(start))
	}
	logger.Log.Debugf("constructSinkCleanupAndPush: ConstructBaseSink START elapsed=%v", time.Since(start))
	baseSink, err := sink_factory.ConstructBaseSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, middlewares.MakeConfig(middlewares.WithNoData))
	defer func() {
		logger.Log.Debugf("constructSinkCleanupAndPush: baseSink.Close START elapsed=%v", time.Since(start))
		require.NoError(t, baseSink.Close())
		logger.Log.Debugf("constructSinkCleanupAndPush: baseSink.Close DONE elapsed=%v", time.Since(start))
	}()

	require.NoError(t, err)
	logger.Log.Debugf("constructSinkCleanupAndPush: ConstructBaseSink DONE elapsed=%v", time.Since(start))
	if completableSink, ok := baseSink.(abstract.Committable); ok {
		logger.Log.Debugf("constructSinkCleanupAndPush: Commit START elapsed=%v", time.Since(start))
		require.NoError(t, completableSink.Commit())
		logger.Log.Debugf("constructSinkCleanupAndPush: Commit DONE elapsed=%v", time.Since(start))
	}
	logger.Log.Debugf("constructSinkCleanupAndPush ALL DONE elapsed=%v", time.Since(start))
}

// ReferenceTestFn returns a function which conducts a reference test with the given items and transfer for all transfer typesystem versions.
//
// Reference test is a canonization test which records the final state of the target database (sink) after a precanonized set of "reference" items has been pushed into the target.
// The final state of the target database is obtained as if it was a snapshot source; that is why a sink-as-source object is required.
//
// This method conducts a cleanup of the target database automatically before each test. Note that transfer's target endpoint should specify cleanup policy DROP or TRUNCATE for this feature to work.
func ReferenceTestFn(transfer *model.Transfer, sinkAsSource model.Source, itemBatches ...[]abstract.ChangeItem) func(*testing.T) {
	allItems := make([]abstract.ChangeItem, 0)
	for i := range itemBatches {
		allItems = append(allItems, itemBatches[i]...)
	}

	tables := helpers.TableMapFromItems(allItems)
	return func(t *testing.T) {
		_caseStart := time.Now()
		logger.Log.Debugf("ReferenceTestFn START tables=%d batches=%d typesystem_versions=%d", len(tables), len(itemBatches), typesystem.LatestVersion)
		startVersion := 1
		// In dynamic table scenario mount/unmount operations take ~1sec, it takes too long to test all reference cases for all typesystems
		if ytDst, ok := transfer.Dst.(*provider_yt.YtDestinationWrapper); ok && !ytDst.Static() {
			startVersion = typesystem.LatestVersion
		}

		for v := startVersion; v <= typesystem.LatestVersion; v++ {
			transfer.TypeSystemVersion = v
			logger.Log.Debugf("ReferenceTestFn: typesystem_%02d START elapsed=%v", v, time.Since(_caseStart))
			t.Run(fmt.Sprintf("typesystem_%02d", v), func(t *testing.T) {
				_tsStart := time.Now()
				logger.Log.Debugf("ReferenceTestFn typesystem_%02d: constructSinkCleanupAndPush START", v)
				constructSinkCleanupAndPush(t, transfer, tables, itemBatches...)
				logger.Log.Debugf("ReferenceTestFn typesystem_%02d: dumpToString START elapsed=%v", v, time.Since(_tsStart))
				result := dumpToString(t, sinkAsSource)
				logger.Log.Debugf("ReferenceTestFn typesystem_%02d: dumpToString DONE len=%d elapsed=%v", v, len(result), time.Since(_tsStart))
				marshalledResult, err := json.Marshal(result)
				require.NoError(t, err)

				cwd, err := os.Getwd()
				require.NoError(t, err)
				fileForResult := filepath.Join(cwd, "result.txt")
				require.NoError(t, os.WriteFile(fileForResult, marshalledResult, 0o666))

				canon.SaveFile(t, fileForResult)
				logger.Log.Debugf("ReferenceTestFn typesystem_%02d ALL DONE elapsed=%v", v, time.Since(_tsStart))
			})
			logger.Log.Debugf("ReferenceTestFn: typesystem_%02d DONE elapsed=%v", v, time.Since(_caseStart))
		}
		logger.Log.Debugf("ReferenceTestFn ALL DONE elapsed=%v", time.Since(_caseStart))
	}
}
