package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/transfer"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	testTableName  = "test_table/my_lovely_table_custom_feed"
	changeFeedName = "changefeed_update_test"
	consumerName   = "consumer_update_test"
)

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:                        model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:                     helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:                     helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:                       nil,
		TableColumnsFilter:           nil,
		SubNetworkID:                 "",
		Underlay:                     false,
		ServiceAccountID:             "",
		UseFullPaths:                 false,
		ChangeFeedCustomName:         changeFeedName,
		ChangeFeedCustomConsumerName: consumerName,
	}

	sinker := mocksink.NewMockSink(nil)
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	var changeItems []abstract.ChangeItem
	mutex := sync.Mutex{}
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		mutex.Lock()
		defer mutex.Unlock()

		for _, currElem := range input {
			if currElem.Kind == abstract.InsertKind || currElem.Kind == abstract.UpdateKind {
				changeItems = append(changeItems, currElem)
			}
		}
		return nil
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

		require.NoError(t, sinker.Push([]abstract.ChangeItem{*testdata.YDBInitChangeItem(testTableName)}))
	})

	// creating changefeed and adding consumer
	ydbClient := ydbrecipe.Driver(t)
	query := fmt.Sprintf("--!syntax_v1\nALTER TABLE `%s` ADD CHANGEFEED %s WITH (FORMAT = 'JSON', MODE = '%s')", testTableName, changeFeedName, provider_ydb.ChangeFeedModeUpdates)
	err := ydbClient.Table().Do(context.Background(), func(ctx context.Context, s ydb_table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, ydb_table.WithIdempotent())
	require.NoError(t, err)

	err = ydbClient.Topic().Alter(
		context.Background(),
		path.Join(testTableName, changeFeedName),
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)

	// running activation
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{testTableName}}
	_, err = helpers.ActivateErr(transfer)
	require.NoError(t, err)
	require.Equal(t, len(changeItems), 1)

	// update source
	t.Run("update source database", func(t *testing.T) {
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		newItem := *testdata.YDBStmtUpdateTOAST(t, testTableName, 1, 11)
		require.NoError(t, sinker.Push([]abstract.ChangeItem{newItem}))
	})

	// check that only updated part is sent
	for {
		time.Sleep(time.Second)

		mutex.Lock()
		if len(changeItems) == 2 {
			break
		}
		mutex.Unlock()
	}
	require.Equal(t, 5, len(changeItems[1].ColumnNames))
}
