package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydb_recipe_table "github.com/transferia/transferia/tests/helpers/ydb_recipe/table"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var pathIn = "dectest/test_snapshot_sharded"
var pathOut = "dectest/test_snapshot_sharded-out"
var parts = map[string]bool{}
var partsCountExpected = 4

//---------------------------------------------------------------------------------------------------------------------

func applyUdf(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	for i := range items {
		items[i].Table = pathOut
		if items[i].Kind == abstract.InsertKind {
			if _, ok := parts[items[i].PartID]; !ok {
				fmt.Printf("changeItem dump:%s\n", items[i].ToJSONString())
				parts[items[i].PartID] = true
			}
		}
	}
	return abstract.TransformerResult{
		Transformed: items,
		Errors:      nil,
	}
}

func anyTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		IsSnapshotSharded:  true,
	}

	t.Run("init source database", func(t *testing.T) {
		ydbConn := ydbrecipe.Driver(t)

		err := ydbConn.Table().Do(context.Background(),
			func(ctx context.Context, s ydb_table.Session) (err error) {
				// create table with four partitions
				tablePath := path.Join(ydbConn.Name(), pathIn)
				err = s.CreateTable(ctx, tablePath,
					ydb_options.WithColumn("c_custkey", ydb_table_types.Optional(ydb_table_types.TypeUint64)),
					ydb_options.WithColumn("random_val", ydb_table_types.Optional(ydb_table_types.TypeUint64)),
					ydb_options.WithPrimaryKeyColumn("c_custkey"),
					ydb_options.WithPartitions(ydb_options.WithUniformPartitions(uint64(partsCountExpected))),
				)
				if err != nil {
					return err
				}
				tableDescription, err := s.DescribeTable(ctx, tablePath, ydb_options.WithShardKeyBounds())
				if err != nil {
					return err
				}

				// insert one row into each partition
				for i, kr := range tableDescription.KeyRanges {
					leftBorder := "1"
					if kr.From != nil {
						leftBorder = kr.From.Yql()
					}
					q := fmt.Sprintf("--!syntax_v1\nUPSERT INTO `%s` (c_custkey, random_val) VALUES  (%s, %d);", tablePath, leftBorder, i)
					fmt.Printf("query to execute ydb:%s\n", q)
					ydb_recipe_table.ExecQuery(t, ydbConn, q)
				}
				return nil
			},
		)
		require.NoError(t, err)
	})

	dst := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst.WithDefaults()
	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly),
		2, 1,
	)

	transformer := helpers.NewSimpleTransformer(t, applyUdf, anyTablesUdf)
	helpers.AddTransformer(t, transfer, transformer)

	t.Run("activate", func(t *testing.T) {
		_, err := helpers.ActivateShardedErr(transfer, nil, nil)
		require.NoError(t, err)
	})
	helpers.CheckRowsCount(t, dst, "", pathOut, 4)
	// check that transfer sent rows asynchronously
	require.Equal(t, partsCountExpected, len(parts))
}
