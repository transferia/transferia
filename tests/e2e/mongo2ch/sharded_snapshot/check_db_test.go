package shardedsnapshot

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
	"github.com/transferia/transferia/tests/helpers"
)

const databaseName string = "db"

var (
	Source = provider_mongo.RecipeSource()
	Target = chrecipe.MustTarget(chrecipe.WithInitFile("dump.sql"), chrecipe.WithDatabase(databaseName))
)

func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mongo source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH HTTP target", Port: Target.HTTPPort},
			helpers.LabeledPort{Label: "CH Native target", Port: Target.NativePort},
		))
	}()

	Source.Collections = []provider_mongo.MongoCollection{
		{DatabaseName: databaseName, CollectionName: "test_data"},
	}
	require.NoError(t, canon_mongo.InsertDocs(context.Background(), Source, databaseName, "test_data", canon_mongo.SnapshotDocuments...))

	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly),
		2,
		1,
	)
	transfer.TypeSystemVersion = 7
	_, err := helpers.ActivateShardedErr(transfer, nil, nil)
	require.NoError(t, err)
	err = helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
		return true
	}).WithPriorityComparators(func(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, intoArray bool) (comparable bool, result bool, err error) {
		ld, _ := json.Marshal(lVal)
		return true, string(ld) == cast.ToString(rVal), nil
	}))
	require.NoError(t, err)
}
