package cdcpartialactivate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	pgrecipe "github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithDBTables("public.__test"))
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

const (
	CursorField = "id"
	CursorValue = "5"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement,
		"public", "__test", CursorField, CursorValue, 15)
	require.NoError(t, transfer.TransformationFromJSON(`
{
	"transformers": [
	  {
		"rawCdcDocGrouper": {
		  "tables": {
			"includeTables": [
				"^public.__test$"
			]
		  },
		  "keys": [
				"aid",
				"id",
                "ts",
                "etl_updated_at"
			],
          "fields": [
                  "str"
           ]
		}
	  }
	]
}
`))
	// start cdc
	worker := helpers.Activate(t, transfer)
	require.NotNil(t, worker, "Transfer is not activated")

	// check snapshot loaded

	conn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer conn.Close()

	expectedYtRows := getExpectedRowsCount(t, conn)
	storage := helpers.GetSampleableStorageByModel(t, Target.LegacyModel())
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test",
		storage, 60*time.Second, expectedYtRows), "Wrong row number after first snapshot round!")

	// add some data to pg
	expectedYtRows = addSomeDataAndGetExpectedCount(t, conn)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test", helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, expectedYtRows))
	worker.Close(t)

	// read data from target
	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "__test",
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		if len(items) == 1 && !items[0].IsRowEvent() {
			return nil
		}
		var deletedCounts int
		for _, row := range items {
			if row.IsRowEvent() {
				require.Len(t, row.TableSchema.Columns(), 7, "Wrong result column count!!")
				require.Equal(t, []string{"aid", "id", "ts", "etl_updated_at", "str", "deleted_flg", "doc"}, row.ColumnNames, "Wrong result column names or order!!")
				require.Equal(t, row.Kind, abstract.InsertKind, "wrong item type!!")
				deletedIndex := row.ColumnNameIndex("deleted_flg")
				if row.ColumnValues[deletedIndex] == true {
					deletedCounts++
				}
			}
		}
		require.Equal(t, 2, deletedCounts, "Deleted rows are not present in target!!")
		return nil
	}))
}

func addSomeDataAndGetExpectedCount(t *testing.T, conn *pgxpool.Pool) uint64 {
	currentDBRows := getCurrentSourceRows(t, conn)

	var extraItems uint64
	_, err := conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 111, '1999-09-16', 1)")
	require.NoError(t, err)
	extraItems++
	_, err = conn.Exec(context.Background(), "update __test set i=2 where str='vvvv';")
	extraItems++ // separate update event
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 111, '1999-09-16', 1),
                                                      ('eee', 111, '1999-09-16', 1),
                                                      ('rrr', 111, '1999-09-16', 1) `)
	require.NoError(t, err)
	extraItems += 3
	_, err = conn.Exec(context.Background(), "delete from __test where str='rrr' or str='eee';")
	require.NoError(t, err)
	extraItems += 2 // item before deletion + deleted event

	return currentDBRows + extraItems
}

func getCurrentSourceRows(t *testing.T, conn *pgxpool.Pool) uint64 {
	var cnt uint64
	err := conn.QueryRow(context.Background(), "select count(*) from __test where id > 5").Scan(&cnt)
	require.NoError(t, err, "Cannot get rows count")
	return cnt
}

func getExpectedRowsCount(t *testing.T, conn *pgxpool.Pool) uint64 {
	return getCurrentSourceRows(t, conn)
}
