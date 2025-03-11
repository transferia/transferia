package chmapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/chmapper"
	"github.com/transferria/transferria/tests/canon/reference"
	"github.com/transferria/transferria/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func testSnapshot(t *testing.T, source *postgres.PgSource, target model.ChDestination) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	Target.ForceJSONMode = false
	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, TransferType)
	transfer.TypeSystemVersion = 8
	transfer.Transformation = &dp_model.Transformation{Transformers: &transformer.Transformers{Transformers: []transformer.Transformer{
		{
			chmapper.TransformerType: chmapper.Config{
				Table: chmapper.TableRemap{
					OriginalTable: abstract.TableID{
						Namespace: "public",
						Name:      "__test",
					},
					RenamedTable: abstract.TableID{
						Namespace: "",
						Name:      "remapped_table",
					},
					Columns: []chmapper.ColumnRemap{
						{Name: "first_id", Path: "id", Key: true, Type: schema.TypeInt64.String(), TargetType: "UInt64"},
						{Name: "second_id", Path: "aid", Key: true, Type: schema.TypeInt32.String(), TargetType: "UInt64"},
						{Name: "third_id", Path: "str", Key: true, Type: schema.TypeString.String(), TargetType: "LowCardinality(String)"},
						{Name: "f", Type: schema.TypeFloat64.String()},
						{Name: "d", Type: schema.TypeFloat64.String()},
						{Name: "i", Type: schema.TypeInt32.String()},
						{Name: "bi", Type: schema.TypeInt64.String()},
						{Name: "biu", Type: schema.TypeUint64.String()},
						{Name: "bit", Type: schema.TypeInt8.String()},
						{Name: "da", Type: schema.TypeDate.String(), TargetType: "Date"},
						{Name: "ts", Type: schema.TypeTimestamp.String(), TargetType: "DateTime(6)"},
						{Name: "dt", Type: schema.TypeDatetime.String(), TargetType: "DateTime(6)"},
						{Name: "c", Type: schema.TypeString.String(), TargetType: "Nullable(String)"},
						{Name: "t", Masked: true, Type: schema.TypeString.String(), TargetType: "String"},
					},
					DDL: &chmapper.DDLParams{
						PartitionBy: "toDate(ts)",
						OrderBy:     "`first_id`, `second_id`, `third_id`",
						Engine:      "MergeTree()",
						TTL:         "toDate(ts) + INTERVAL 700 year",
					},
				},
				Salt: "test",
			},
		},
	}}}
	helpers.Activate(t, transfer, func(err error) {
		require.NoError(t, err)
	})
	reference.Dump(t, &model.ChSource{
		Database:   "public",
		ShardsList: []model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		NativePort: target.NativePort,
		HTTPPort:   target.HTTPPort,
		User:       target.User,
	})
	require.NoError(t,
		helpers.WaitEqualRowsCountDifferentTables(
			t,
			"public",
			"__test",
			target.Database,
			"remapped_table",
			helpers.GetSampleableStorageByModel(t, Source),
			helpers.GetSampleableStorageByModel(t, Target),
			10*time.Second,
		),
	)
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
