package snapshot

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	client2 "github.com/transferria/transferria/pkg/abstract/coordinator"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	mongocommon "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/canon/mongo"
	"github.com/transferria/transferria/tests/helpers"
)

const databaseName string = "db"

var (
	Source = mongocommon.RecipeSource()
	Target = chrecipe.MustTarget(chrecipe.WithInitFile("dump.sql"), chrecipe.WithDatabase(databaseName))
)

func MakeDstClient(t *mongocommon.MongoDestination) (*mongocommon.MongoClientWrapper, error) {
	return mongocommon.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH HTTP target", Port: Target.HTTPPort},
			helpers.LabeledPort{Label: "CH Native target", Port: Target.NativePort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Snapshot", Snapshot)
	})
}

func Ping(t *testing.T) {
	client, err := mongocommon.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	Source.Collections = []mongocommon.MongoCollection{
		{DatabaseName: databaseName, CollectionName: "test_data"},
		{DatabaseName: databaseName, CollectionName: "empty"},
	}

	require.NoError(t, mongo.InsertDocs(context.Background(), Source, databaseName, "test_data", mongo.SnapshotDocuments...))

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 7
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	snapshotLoader := tasks.NewSnapshotLoader(client2.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	err = helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
		return true
	}).WithPriorityComparators(func(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, intoArray bool) (comparable bool, result bool, err error) {
		ld, _ := json.Marshal(lVal)
		return true, string(ld) == cast.ToString(rVal), nil
	}))
	require.NoError(t, err)
}
