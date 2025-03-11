package snapshot

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/pkg/transformer"
	batchsplitter "github.com/transferria/transferria/pkg/transformer/registry/batch_splitter"
	"github.com/transferria/transferria/tests/helpers"
)

var expectedChangeItemsCount = 10
var maxBatchSize = 1

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		var changes []abstract.ChangeItem
		for i := 1; i <= expectedChangeItemsCount; i++ {
			changes = append(changes, *helpers.YDBStmtInsert(t, "test/batch_splitter_test", i))
		}
		require.NoError(t, sinker.Push(changes))
	})

	sinker := &helpers.MockSink{}
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	mutex := sync.Mutex{}
	var changeItemsCount int
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		mutex.Lock()
		defer mutex.Unlock()
		require.Equal(t, maxBatchSize, len(input))
		if input[0].Kind == abstract.InsertKind {
			changeItemsCount += 1
		}
	}

	// create transfer with batch-splitter transformer
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	transfer.Transformation = &model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			batchsplitter.Type: batchsplitter.Config{
				MaxItemsPerBatch: 1,
			},
		}},
		ErrorsOutput: nil,
	}}

	helpers.Activate(t, transfer)
	require.Equal(t, expectedChangeItemsCount, changeItemsCount)
}
