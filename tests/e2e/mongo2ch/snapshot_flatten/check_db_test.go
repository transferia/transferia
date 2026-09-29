package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_mongo "github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/transformer"
	transformer_clickhouse "github.com/transferia/transferia/pkg/transformer/registry/clickhouse"
	transformer_filter "github.com/transferia/transferia/pkg/transformer/registry/filter"
	canon_mongo "github.com/transferia/transferia/tests/canon/mongo"
	canon_reference "github.com/transferia/transferia/tests/canon/reference"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/mongo"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"go.mongodb.org/mongo-driver/bson"
)

const databaseName string = "db"

var (
	Source = provider_mongo.RecipeSource()
	Target = chrecipe.MustTarget(chrecipe.WithInitFile("dump.sql"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
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
	Target.ChClusterName = ""

	doc := `{
  "_id": "D1AAD9AB",
  "floors": [
    {
      "currency": "EUR",
      "value": 0.2,
      "countryIds": [
        "IT"
      ]
    },
    {
      "currency": "EUR",
      "value": 0.3,
      "countryIds": [
        "FR",
        "GB"
      ]
    }
  ]
}`
	var masterDoc bson.D
	require.NoError(t, bson.UnmarshalExtJSON([]byte(doc), false, &masterDoc))

	require.NoError(t, canon_mongo.InsertDocs(context.Background(), Source, databaseName, "test_data", masterDoc))

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = 7
	transfer.Transformation = &model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			transformer_clickhouse.Type: transformer_clickhouse.Config{
				Tables: transformer_filter.Tables{
					IncludeTables: []string{fmt.Sprintf("%s.%s", databaseName, "test_data")},
				},
				Query: `
SELECT _id,
	JSONExtractArrayRaw(document,'floors') as floors_as_string_array,
	arrayMap(x -> JSONExtractFloat(x, 'value'), JSONExtractArrayRaw(document,'floors')) as value_from_floors,
	arrayMap(x -> JSONExtractString(x, 'currency'), JSONExtractArrayRaw(document,'floors')) as currency_from_floors,
	JSONExtractRaw(assumeNotNull(document),'floors') AS floors_as_string
FROM table
SETTINGS
    function_json_value_return_type_allow_nullable = true,
    function_json_value_return_type_allow_complex = true
`,
			},
		}},
		ErrorsOutput: nil,
	}}
	delivery.Activate(t, transfer)

	canon.SaveJSON(t, canon_reference.FromClickhouse(t, &clickhouse_model.ChSource{
		Database:   databaseName,
		ShardsList: []clickhouse_model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		NativePort: Target.NativePort,
		HTTPPort:   Target.HTTPPort,
		User:       Target.User,
	}, true))
}
