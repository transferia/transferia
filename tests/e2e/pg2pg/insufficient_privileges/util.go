package insufficientprivileges

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"go.ytsaurus.tech/library/go/core/log"
)

func newSource(includeTables, excludeTables []string) *pgcommon.PgSource {
	source := pgrecipe.RecipeSource()
	source.User = "loser"
	source.Password = "123"
	source.DBTables = includeTables
	source.ExcludedTables = excludeTables
	source.SlotID = ""
	return source
}

func newTarget() *pgcommon.PgDestination {
	return pgrecipe.RecipeTarget()
}

type tableRow struct {
	id    int
	value string
}

func makeConnConfig(dbPort int) *pgx.ConnConfig {
	config, _ := pgx.ParseConfig("")
	config.Port = uint16(dbPort)

	source := pgrecipe.RecipeSource()
	config.Host = "localhost"
	config.Database = source.Database
	config.User = source.User
	config.Password = string(source.Password)
	config.PreferSimpleProtocol = true

	return config
}

func exec(t *testing.T, dbPort int, query string, params ...interface{}) {
	var logger log.Logger = nil
	connPool, err := pgcommon.NewPgConnPool(makeConnConfig(dbPort), logger)
	require.NoError(t, err)

	_, err = connPool.Exec(context.Background(), query, params...)
	require.NoError(t, err)
}

func queryRow(t *testing.T, dbPort int, query string, outValue interface{}) {
	var logger log.Logger = nil
	connPool, err := pgcommon.NewPgConnPool(makeConnConfig(dbPort), logger)
	require.NoError(t, err)

	err = connPool.QueryRow(context.Background(), query).Scan(outValue)
	require.NoError(t, err)
}

func createTable(t *testing.T, dbPort int, tableName string) {
	exec(t, dbPort, fmt.Sprintf(`CREATE TABLE %s (id INTEGER PRIMARY KEY, value TEXT)`, tableName))
}

func insertOneRow(t *testing.T, dbPort int, tableName string, row tableRow) {
	exec(t, dbPort, fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2)`, tableName), row.id, row.value)
}

func rowCount(t *testing.T, dbPort int, tableName string) int {
	var rowCount int
	queryRow(t, dbPort, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName), &rowCount)
	return rowCount
}

func grantPrivileges(t *testing.T, dbPort int, tableName, userName string) {
	exec(t, dbPort, fmt.Sprintf(`GRANT ALL PRIVILEGES ON TABLE %s TO %s;`, tableName, userName))
}
