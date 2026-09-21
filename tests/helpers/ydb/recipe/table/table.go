package ydbtable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func ExecQuery(t *testing.T, driver *ydb_go_sdk.Driver, query string) {
	ExecQueries(t, driver, []string{query})
}

func ExecQueries(t *testing.T, driver *ydb_go_sdk.Driver, queries []string) {
	require.NoError(t, driver.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
		writeTx := ydb_table.TxControl(
			ydb_table.BeginTx(
				ydb_table.WithSerializableReadWrite(),
			),
			ydb_table.CommitTx(),
		)

		for _, query := range queries {
			if _, _, err := session.Execute(ctx, writeTx, query, nil); err != nil {
				return err
			}
		}
		return nil
	}))
}
