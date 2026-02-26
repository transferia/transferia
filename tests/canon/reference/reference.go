package reference

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func constructSinkCleanupAndPush(t *testing.T, transfer *model.Transfer, tables abstract.TableMap, itemBatches ...[]abstract.ChangeItem) {
	cp := coordinator.NewStatefulFakeClient()
	as, err := sink.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)
	defer func() { require.NoError(t, as.Close()) }()

	snapshotLoader := tasks.NewSnapshotLoader(cp, &model.TransferOperation{}, transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.CleanupSinker(tables))

	for i := range itemBatches {
		errCh := as.AsyncPush(itemBatches[i])
		require.NoError(t, <-errCh)
	}
	baseSink, err := sink.ConstructBaseSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, middlewares.MakeConfig(middlewares.WithNoData))
	defer func() {
		require.NoError(t, baseSink.Close())
	}()

	require.NoError(t, err)
	if completableSink, ok := baseSink.(abstract.Committable); ok {
		require.NoError(t, completableSink.Commit())
	}
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
		startVersion := 1
		// In dynamic table scenario mount/unmount operations take ~1sec, it takes too long to test all reference cases for all typesystems
		if ytDst, ok := transfer.Dst.(*yt.YtDestinationWrapper); ok && !ytDst.Static() {
			startVersion = typesystem.LatestVersion
		}

		for v := startVersion; v <= typesystem.LatestVersion; v++ {
			transfer.TypeSystemVersion = v
			t.Run(fmt.Sprintf("typesystem_%02d", v), func(t *testing.T) {
				constructSinkCleanupAndPush(t, transfer, tables, itemBatches...)
				result := dumpToString(t, sinkAsSource)
				marshalledResult, err := json.Marshal(result)
				require.NoError(t, err)

				cwd, err := os.Getwd()
				require.NoError(t, err)
				fileForResult := filepath.Join(cwd, "result.txt")
				require.NoError(t, os.WriteFile(fileForResult, marshalledResult, 0o666))

				canon.SaveFile(t, fileForResult)
			})
		}
	}
}
