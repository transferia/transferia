package main

import (
	"os"
	"sort"
	"strings"
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
	"github.com/transferia/transferia/tests/helpers/serde"
	simple_transformer "github.com/transferia/transferia/tests/helpers/transformer"
)

var path = "dectest/test-src"

func TestCompareSnapshotAndReplication(t *testing.T) {
	var extractedFromReplication []abstract.ChangeItem
	var extractedFromSnapshot []abstract.ChangeItem

	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
	}

	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsert(t, path, 1),
		*helpers.YDBStmtDelete(t, path, 1),
	}))
	// replication
	sinkMock := &helpers.MockSink{}
	sinkMock.PushCallback = func(input []abstract.ChangeItem) error {
		for _, currItem := range input {
			if currItem.Kind == abstract.UpdateKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extractedFromReplication = append(extractedFromReplication, currItem)
			} else if currItem.Kind == abstract.InsertKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extractedFromSnapshot = append(extractedFromSnapshot, currItem)
			}
		}
		return nil
	}
	targetMock := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       model.DisabledCleanup,
	}

	transfer := helpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeIncrementOnly)
	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithoutCheck(emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	worker := helpers.Activate(t, transfer)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertNulls(t, path, 1),
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues2, 2),
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues3, 3),
	}))

	require.NoError(t, helpers.WaitCond(time.Second*60, func() bool {
		return len(extractedFromReplication) == 3
	}))
	worker.Close(t)

	transferSnapshot := helpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, transferSnapshot.AddExtraTransformer(debeziumSerDeTransformer))
	helpers.Activate(t, transferSnapshot)

	// compare

	require.Equal(t, len(extractedFromReplication), len(extractedFromSnapshot))
	sort.Slice(extractedFromReplication, func(i, j int) bool {
		return strings.Join(extractedFromReplication[i].KeyVals(), ".") < strings.Join(extractedFromReplication[j].KeyVals(), ".")
	})
	sort.Slice(extractedFromSnapshot, func(i, j int) bool {
		return strings.Join(extractedFromSnapshot[i].KeyVals(), ".") < strings.Join(extractedFromSnapshot[j].KeyVals(), ".")
	})
	for i := 0; i < len(extractedFromSnapshot); i++ {
		extractedFromSnapshot[i].CommitTime = 0
		extractedFromReplication[i].CommitTime = 0
		extractedFromSnapshot[i].PartID = ""
		extractedFromReplication[i].PartID = ""
		snapshot := extractedFromSnapshot[i].AsMap()
		replica := extractedFromReplication[i].AsMap()
		for key, value := range snapshot {
			require.Equal(t, replica[key], value)
		}
	}
	canon.SaveJSON(t, struct {
		FromSnapshot []abstract.ChangeItem
		FromReplica  []abstract.ChangeItem
	}{
		FromSnapshot: extractedFromSnapshot,
		FromReplica:  extractedFromReplication,
	})
}
