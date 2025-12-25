package snapshot

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func checkIfDebeziumConvertorWorks(t *testing.T, currChangeItem *abstract.ChangeItem) {
	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "ydb",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	arrKV, err := emitter.EmitKV(currChangeItem, time.Time{}, false, nil)
	require.NoError(t, err)
	for _, kv := range arrKV {
		logger.Log.Infof("timmyb32rQQQ:DBZ:KEY=%s\n", kv.DebeziumKey)
		if kv.DebeziumVal != nil {
			logger.Log.Infof("timmyb32rQQQ:DBZ:VAL=%s\n", *kv.DebeziumVal)
		} else {
			logger.Log.Infof("timmyb32rQQQ:DBZ:VAL=NULL\n")
		}
	}
}

func Iteration(t *testing.T, currMode ydb.ChangeFeedModeType) map[string]interface{} {
	currTableName := fmt.Sprintf("foo/my_table_%v", string(currMode))
	logger.Log.Infof("current table name: %s\n", currTableName)

	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     currMode,
		UseFullPaths:       true,
	}

	sink := mocksink.NewMockSink(nil)
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sink },
		Cleanup:       model.DisabledCleanup,
	}

	result := make(map[string]interface{})

	index := 0
	sink.PushCallback = func(input []abstract.ChangeItem) error {
		for _, currChangeItem := range input {
			if currChangeItem.Kind == abstract.InsertKind || currChangeItem.Kind == abstract.UpdateKind || currChangeItem.Kind == abstract.DeleteKind {
				index++

				logger.Log.Infof("changeItem:%s\n", currChangeItem.ToJSONString())

				// check if there are only 1 element in every oldKeys
				if currMode == ydb.ChangeFeedModeUpdates || currMode == ydb.ChangeFeedModeNewImage {
					require.Len(t, currChangeItem.OldKeys.KeyNames, 1)
					require.Len(t, currChangeItem.OldKeys.KeyValues, 1)
					require.Len(t, currChangeItem.OldKeys.KeyTypes, 1)
				}

				checkIfDebeziumConvertorWorks(t, &currChangeItem)

				currChangeItem.CommitTime = 0
				result[fmt.Sprintf("%v-%v", currMode, index)] = currChangeItem
			}
		}
		return nil
	}

	// init source table

	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	srcSink, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{ // to create table
		*helpers.YDBStmtInsert(t, currTableName, 1),
		*helpers.YDBStmtInsertNulls(t, currTableName, 2),
	}))

	// start replication

	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// write into source once row

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertNulls(t, currTableName, 3),
		*helpers.YDBStmtInsert(t, currTableName, 4),
	}))

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdate(t, currTableName, 4, 666),
	}))
	helpers.CheckRowsCount(t, src, "", currTableName, 4)

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdateTOAST(t, currTableName, 4, 777),
	}))
	helpers.CheckRowsCount(t, src, "", currTableName, 4)

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtDelete(t, currTableName, 1),
	}))
	helpers.CheckRowsCount(t, src, "", currTableName, 3)

	// wait when all events goes thought sink

	for {
		if len(result) == 5 {
			break
		}
		time.Sleep(time.Second)
	}

	return result
}

func TestCRUDOnAllSupportedModes(t *testing.T) {
	modes := []ydb.ChangeFeedModeType{
		//ydb.ChangeFeedModeUpdates,
		ydb.ChangeFeedModeNewImage,
		ydb.ChangeFeedModeNewAndOldImages,
	}
	canonResult := make([]map[string]interface{}, 0)
	for _, currMode := range modes {
		canonResultEL := Iteration(t, currMode)
		canonResult = append(canonResult, canonResultEL)
	}
	canon.SaveJSON(t, canonResult)
}
