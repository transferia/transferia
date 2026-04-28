package snapshot

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"go.ytsaurus.tech/library/go/core/log"
)

// GetRowsCount returns NUM_ROWS from all_tables or falls back to COUNT(*).
func GetRowsCount(logger log.Logger, config *provider_oracle.OracleSource, sqlxDB *sqlx.DB, table *oracle_schema.Table) (uint64, error) {
	count := new(uint64)

	queryErr := oracle_common.PDBQueryGlobal(config, sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			return connection.GetContext(ctx, &count,
				"select num_rows from all_tables where owner = :schema_name and table_name = :table_name",
				table.OracleSchema().OracleName(), table.OracleName())
		})

	if queryErr != nil || count == nil {
		if queryErr != nil {
			logger.Warnf("Can't get exemplary rows count from table '%v': %v", table.OracleSQLName(), queryErr)
		} else {
			logger.Warnf("Can't get exemplary rows count from table '%v': count is nil", table.OracleSQLName())
		}

		queryErr = oracle_common.PDBQueryGlobal(config, sqlxDB, context.Background(),
			func(ctx context.Context, connection *sqlx.Conn) error {
				return connection.GetContext(ctx, &count, fmt.Sprintf("select count(*) from %v", table.OracleSQLName()))
			})

		if queryErr != nil {
			return 0, xerrors.Errorf("Can't get rows count from table '%v': %w", table.OracleSQLName(), queryErr)
		}
	}

	return *count, nil
}

func getSelectColumns(table *oracle_schema.Table) (string, error) {
	columnsSQLBuilder := strings.Builder{}
	for i := 0; i < table.ColumnsCount(); i++ {
		column := table.OracleColumn(i)
		columnSQL, err := column.OracleSQLSelect()
		if err != nil {
			return "", xerrors.Errorf("Can't create SQL for select column '%v': %w", column.OracleSQLName(), err)
		}

		if columnsSQLBuilder.Len() > 0 {
			columnsSQLBuilder.WriteString(", ")
		}

		columnsSQLBuilder.WriteString(columnSQL)
	}
	return columnsSQLBuilder.String(), nil
}
