package snapshot

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/transformer"
	transformer_batch_splitter "github.com/transferia/transferia/pkg/transformer/registry/batch_splitter"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

var expectedChangeItemsCount = 10
var maxBatchSize = 1

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		var changes []abstract.ChangeItem
		for i := 1; i <= expectedChangeItemsCount; i++ {
			changes = append(changes, *testdata.YDBStmtInsert(t, "test/batch_splitter_test", i))
		}
		require.NoError(t, sinker.Push(changes))
	})

	sinker := mocksink.NewMockSink(nil)
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	mutex := sync.Mutex{}
	var changeItemsCount int
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		mutex.Lock()
		defer mutex.Unlock()
		require.Equal(t, maxBatchSize, len(input))
		if input[0].Kind == abstract.InsertKind {
			changeItemsCount += 1
		}
		return nil
	}

	// create transfer with batch-splitter transformer
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	transfer.Transformation = &model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			transformer_batch_splitter.Type: transformer_batch_splitter.Config{
				MaxItemsPerBatch: 1,
			},
		}},
		ErrorsOutput: nil,
	}}

	delivery.Activate(t, transfer)
	require.Equal(t, expectedChangeItemsCount, changeItemsCount)
}
