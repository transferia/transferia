package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	mongocommon "github.com/transferria/transferria/pkg/providers/mongo"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/clickhouse"
	"github.com/transferria/transferria/pkg/transformer/registry/filter"
	"github.com/transferria/transferria/tests/canon/mongo"
	"github.com/transferria/transferria/tests/canon/reference"
	"github.com/transferria/transferria/tests/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

const databaseName string = "db"

var (
	Source = mongocommon.RecipeSource()
	Target = chrecipe.MustTarget(chrecipe.WithInitFile("dump.sql"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
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

	require.NoError(t, mongo.InsertDocs(context.Background(), Source, databaseName, "test_data", masterDoc))

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = 7
	transfer.Transformation = &dp_model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			clickhouse.Type: clickhouse.Config{
				Tables: filter.Tables{
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
	helpers.Activate(t, transfer)

	canon.SaveJSON(t, reference.FromClickhouse(t, &model.ChSource{
		Database:   databaseName,
		ShardsList: []model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		NativePort: Target.NativePort,
		HTTPPort:   Target.HTTPPort,
		User:       Target.User,
	}, true))
}
