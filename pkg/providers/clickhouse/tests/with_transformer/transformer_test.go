//go:build !disable_clickhouse_provider

package withtransformer

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/sink"
	clickhouse_transformer "github.com/transferia/transferia/pkg/transformer/registry/clickhouse"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/tests/canon/reference"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	source         = model.MockSource{}
	target         = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
	targetAsSource = *chrecipe.MustSource(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
}

func TestTransformerTypeInference(t *testing.T) {
	// mongo-like schema
	sch := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableName:  "table",
			ColumnName: "_id",
			DataType:   schema.TypeString.String(), // Downcast int32 -> int16
			PrimaryKey: true,
		},
		{
			TableName:  "table",
			ColumnName: "document",
			DataType:   schema.TypeAny.String(), // Downcast int64 -> int32
		},
	})

	query := `
SELECT _id,
JSONExtractString(assumeNotNull(document),'name') AS name,
JSONExtractBool(assumeNotNull(document),'isActive') AS isActive,
JSONExtractBool(assumeNotNull(document),'isArchived') AS isArchived,
JSONExtractArrayRaw(assumeNotNull(document),'tagIds') AS tagIdsAsArray
FROM table
`
	sample := `{
  "_id": "23",
  "createdAt": {
    "$date": "2023-04-19T05:45:55.093Z"
  },
  "lastModifiedAt": {
    "$date": "2023-04-19T05:45:55.093Z"
  },
  "accountIds": [
    "36"
  ],
  "primaryAccountId": "36",
  "lastModifiedBy": "admin",
  "name": "D - Le Lab Virgin Radio ",
  "packType": "Technical",
  "externalId": "vr-785959",
  "isActive": true,
  "isArchived": false,
  "createdBy": "admin",
  "tagIds": [
    "5f6da21bdbe1f",
    "5f6da23aaf579"
  ]
}`
	var doc any
	require.NoError(t, json.Unmarshal([]byte(sample), &doc))

	items := []abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []any{"test", doc},
			TableSchema:  sch,
		},
	}
	transformer, err := clickhouse_transformer.New(clickhouse_transformer.Config{
		Tables: filter.Tables{IncludeTables: []string{".*"}, ExcludeTables: nil},
		Query:  query,
	}, logger.Log)
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, transfer.AddExtraTransformer(transformer))
	sinker, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig())
	require.NoError(t, err)
	require.NoError(t, <-sinker.AsyncPush(items))
	reference.Dump(t, &targetAsSource)
}
