package sharding

import (
	"context"
	_ "embed"
	"testing"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
)

//go:embed source.sql
var sourceDB []byte

func TestShardingByPartitions(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	if source.Database == "" {
		// init database
		source.Database = "source"
	}
	connectionParams, err := provider_mysql.NewConnectionParams(source.ToStorageParams())
	require.NoError(t, err)
	db, err := provider_mysql.Connect(connectionParams, func(config *mysql_driver2.Config) error {
		config.MultiStatements = true
		return nil
	})
	require.NoError(t, err)
	_, err = db.Exec(string(sourceDB))
	require.NoError(t, err)
	storage, err := provider_mysql.NewStorage(source.ToStorageParams(), testmetrics.EmptyRegistry())
	require.NoError(t, err)
	parts, err := storage.ShardTable(context.Background(), abstract.TableDescription{
		Name:   "orders",
		Schema: source.Database,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	require.NoError(t, err)
	require.Len(t, parts, 4)
	resRows := 0
	for _, part := range parts {
		require.NoError(
			t,
			storage.LoadTable(context.Background(), part, func(items []abstract.ChangeItem) error {
				for _, r := range items {
					if r.IsRowEvent() {
						resRows++
					}
				}
				return nil
			}),
		)
	}
	require.Equal(t, resRows, 6)
}
