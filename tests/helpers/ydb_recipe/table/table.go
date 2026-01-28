package ydbtable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func ExecQuery(t *testing.T, driver *ydb.Driver, query string) {
	ExecQueries(t, driver, []string{query})
}

func ExecQueries(t *testing.T, driver *ydb.Driver, queries []string) {
	require.NoError(t, driver.Table().Do(context.Background(), func(ctx context.Context, session table.Session) error {
		writeTx := table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)

		for _, query := range queries {
			if _, _, err := session.Execute(ctx, writeTx, query, nil); err != nil {
				return err
			}
		}
		return nil
	}))
}
