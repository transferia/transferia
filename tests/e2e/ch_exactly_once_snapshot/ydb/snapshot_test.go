package ydb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	driver := ydbrecipe.Driver(test)
	err := driver.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
		if err := session.ExecuteSchemeQuery(ctx, "CREATE TABLE rows (id Int64, payload Utf8, PRIMARY KEY(id));"); err != nil {
			return err
		}
		return session.ExecuteSchemeQuery(ctx, "CREATE TABLE empty (id Int64, payload Utf8, PRIMARY KEY(id));")
	})
	require.NoError(test, err)
	for rowIndex := 1; rowIndex <= 257; rowIndex++ {
		err = driver.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
			_, _, err := session.Execute(ctx, ydb_table.DefaultTxControl(), fmt.Sprintf("UPSERT INTO rows (id,payload) VALUES (%d, 'row-%d');", rowIndex, rowIndex), nil)
			return err
		})
		require.NoError(test, err)
	}
	eo.ForDatabases(test, func(test *testing.T) {
		for _, sharded := range []bool{false, true} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("sharded%t/workers%d", sharded, workers), func(test *testing.T) {
					source := &provider_ydb.YdbSource{Database: os.Getenv("YDB_DATABASE"), Instance: os.Getenv("YDB_ENDPOINT"), Tables: []string{"rows", "empty"}, IsSnapshotSharded: sharded}
					target := eo.NewTarget(test, "")
					eo.Run(test, source, target, workers)
					eo.CheckRows(test, target, "rows", 257)
					eo.CheckRows(test, target, "empty", 0)
					eo.CheckKeyPayload(test, target, "rows", 257)
				})
			}
		}
	})
}
