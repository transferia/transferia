package snapshot

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	canon_mongo "github.com/transferia/transferia/tests/canon/mongo"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

const databaseName string = "db"

var (
	Source = provider_mongo.RecipeSource()
	Target = chrecipe.MustTarget(chrecipe.WithInitFile("dump.sql"), chrecipe.WithDatabase(databaseName))
)

func MakeDstClient(t *provider_mongo.MongoDestination) (*provider_mongo.MongoClientWrapper, error) {
	return provider_mongo.Connect(context.Background(), t.ConnectionOptions([]string{}), nil)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mongo source", Port: Source.Port},
			network.LabeledPort{Label: "CH HTTP target", Port: Target.HTTPPort},
			network.LabeledPort{Label: "CH Native target", Port: Target.NativePort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Ping", Ping)
		t.Run("Snapshot", Snapshot)
	})
}

func Ping(t *testing.T) {
	client, err := provider_mongo.Connect(context.Background(), Source.ConnectionOptions([]string{}), nil)
	require.NoError(t, err)
	err = client.Ping(context.TODO(), nil)
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	Source.Collections = []provider_mongo.MongoCollection{
		{DatabaseName: databaseName, CollectionName: "test_data"},
	}

	require.NoError(t, canon_mongo.InsertDocs(context.Background(), Source, databaseName, "test_data", canon_mongo.SnapshotDocuments...))

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 7

	_ = delivery.Activate(t, transfer)
	err := storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
		return true
	}).WithPriorityComparators(func(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, intoArray bool) (comparable bool, result bool, err error) {
		ld, _ := json.Marshal(lVal)
		return true, string(ld) == cast.ToString(rVal), nil
	}))
	require.NoError(t, err)
}
