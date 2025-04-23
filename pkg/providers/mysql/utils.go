package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/unmarshaller/snapshot"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/size"
	"go.ytsaurus.tech/library/go/core/log"
)

func makeMapColNameToColTypeName(ctx context.Context, tx Queryable, tableName string) (map[string]string, error) {
	colNameToColTypeName := map[string]string{}

	rowsCol, err := tx.QueryContext(
		ctx,
		"SELECT a.column_name, a.column_type FROM information_schema.columns a WHERE a.TABLE_NAME = ?;",
		tableName,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to select column types: %w", err)
	}

	for rowsCol.Next() {
		var columnName, columnType string
		if err := rowsCol.Scan(&columnName, &columnType); err != nil {
			return nil, xerrors.Errorf("unable to parse column info row: %w", err)
		}
		colNameToColTypeName[columnName] = columnType
	}

	if err := rowsCol.Close(); err != nil {
		return nil, xerrors.Errorf("unable to close column info rows: %w", err)
	}
	return colNameToColTypeName, nil
}

func makeArrColsNames(tableSchema []abstract.ColSchema) []string {
	colsNames := make([]string, len(tableSchema))
	for i, col := range tableSchema {
		colsNames[i] = col.ColumnName
	}
	return colsNames
}

func calcChunkSize(totalRows uint64, totalSize uint64, initialChunkSize uint64) uint64 {
	chunkSize := initialChunkSize
	if totalRows != 0 {
		desiredChunk := uint64(float64(totalRows) / (float64(totalSize) / float64(50*1024*1024)))
		chunkSize = uint64(100000)
		if desiredChunk < chunkSize && totalSize != 0 {
			if desiredChunk < 100 {
				chunkSize = 100
			} else {
				chunkSize = desiredChunk
			}
		}
	}
	return chunkSize
}

func getTableRowsCountTableDataSizeInBytes(ctx context.Context, tx Queryable, table abstract.TableDescription) (uint64, uint64, error) {
	var err error
	var tableRowsCount uint64
	err = tx.
		QueryRowContext(ctx, `
		SELECT
			COALESCE(TABLE_ROWS, 0)
		FROM information_schema.tables
		WHERE TABLE_NAME = ? AND table_schema = ?`, table.Name, table.Schema).
		Scan(&tableRowsCount)
	if err != nil {
		return 0, 0, xerrors.Errorf("unable to select table rows count: %w", err)
	}

	var tableDataSizeInBytes uint64
	err = tx.
		QueryRowContext(ctx, `
		SELECT
			COALESCE(data_length, 0)
		FROM information_schema.tables
		WHERE TABLE_NAME = ? AND table_schema = ?`, table.Name, table.Schema).
		Scan(&tableDataSizeInBytes)
	if err != nil {
		return 0, 0, xerrors.Errorf("unable to select table data size: %w", err)
	}

	return tableRowsCount, tableDataSizeInBytes, nil
}

//---------------------------------------------------------------------------------------------------------------------
// SQL utils
//
// We intentionally not to sanitize sql, bcs tableNames taken from information_schema
// and here are direct substring from user: 'table.Filter'
//
// User acts with db, where he has access.
// If user can break his db this way - he is evil Pinocchio

func buildSelectQuery(table abstract.TableDescription, tableSchema []abstract.ColSchema) string {
	colNames := MakeArrBacktickedColumnNames(&tableSchema)

	resultQuery := fmt.Sprintf(
		"SELECT %v FROM `%v`.`%v` ",
		strings.Join(colNames, ", "),
		table.Schema,
		table.Name,
	)

	if table.Filter != "" {
		if IsPartition(table.Filter) {
			// we use partition-by sharding mechanism, to query exact partition there is no need to use `where` key-word
			// see: https://stackoverflow.com/questions/14112283/how-to-select-rows-from-partition-in-mysql
			resultQuery += string(table.Filter)
		} else {
			resultQuery += " WHERE " + string(table.Filter)
		}
	}
	if table.Offset != 0 {
		resultQuery += fmt.Sprintf(" OFFSET %d", table.Offset)
	}

	return resultQuery
}

func IsPartition(filter abstract.WhereStatement) bool {
	return strings.HasPrefix(string(filter), "PARTITION")
}

func MakeArrBacktickedColumnNames(tableSchema *[]abstract.ColSchema) []string {
	colNames := make([]string, len(*tableSchema))
	for idx, col := range *tableSchema {
		colNames[idx] = fmt.Sprintf("`%s`", col.ColumnName)
	}
	return colNames
}

func OrderByPrimaryKeys(tableSchema []abstract.ColSchema, direction string) (string, error) {
	var keys []string
	for _, col := range tableSchema {
		if col.PrimaryKey {
			keys = append(keys, fmt.Sprintf("`%s` %s", col.ColumnName, direction))
		}
	}
	if len(keys) == 0 {
		return "", xerrors.New("No primary key columns found")
	}
	return " ORDER BY " + strings.Join(keys, ","), nil
}

//---------------------------------------------------------------------------------------------------------------------

type sqlRows interface {
	ColumnTypes() ([]*sql.ColumnType, error)
	Next() bool
	Scan(dest ...interface{}) error
}

func prepareArrayWithTypes(rows sqlRows, colNameToColTypeName map[string]string, location *time.Location) ([]interface{}, error) {
	columnTypesFromQuery, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(columnTypesFromQuery))
	for i := range columnTypesFromQuery {
		result[i] = snapshot.NewValueReceiver(
			columnTypesFromQuery[i],
			colNameToColTypeName[columnTypesFromQuery[i].Name()],
			location)
	}
	return result, nil
}

func pushCreateTable(ctx context.Context, tx Queryable, table abstract.TableID, st time.Time, pusher abstract.Pusher) error {
	ddlItem := ddlValue{
		statement:  "",
		commitTime: st,
		schema:     table.Namespace,
		name:       "",
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%v`.`%v`;", table.Namespace, table.Name)
	err := tx.QueryRowContext(ctx, query).Scan(&ddlItem.name, &ddlItem.statement)
	if err != nil {
		return xerrors.Errorf("unable to get create table statement: %w", err)
	}
	ddlItem.statement = strings.Replace(ddlItem.statement, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	return applyDDLs([]ddlValue{ddlItem}, pusher)
}

// fileOffset for next file we add this number to LSN
const fileOffset = 1_000_000_000_000

func CalculateLSN(file string, pos uint64) uint64 {
	parts := strings.Split(file, ".")
	if len(parts) <= 1 {
		return fileOffset + pos
	}
	fIdx, _ := strconv.Atoi(parts[1])
	return uint64(fIdx*fileOffset) + pos
}

func readRowsAndPushByChunks(
	location *time.Location,
	rows sqlRows,
	st time.Time,
	table abstract.TableDescription,
	tableSchema *abstract.TableSchema,
	colNameToColTypeName map[string]string,
	chunkSize uint64,
	lsn uint64,
	isHomo bool,
	pusher abstract.Pusher) error {

	colsNames := makeArrColsNames(tableSchema.Columns())

	values, err := prepareArrayWithTypes(rows, colNameToColTypeName, location)
	if err != nil {
		return xerrors.Errorf("unable to prepare row values: %w", err)
	}

	inflight := make([]abstract.ChangeItem, 0)
	globalIdx := uint64(0)
	inflightSize := uint64(0)
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			msg := "unable to scan row values"
			logger.Log.Error(msg, log.Error(err))
			return xerrors.Errorf("%v: %w", msg, err)
		}
		readValuesSize := util.DeepSizeof(values)
		inflightSize += readValuesSize

		var columnValues []any
		if isHomo {
			columnValues, err = snapshot.UnmarshalHomo(values, tableSchema.Columns())
		} else {
			columnValues, err = snapshot.UnmarshalHetero(values, tableSchema.Columns())
		}
		if err != nil {
			return xerrors.Errorf("failed to unmarshal a row from MySQL resultset for table %s: %w", table.Fqtn(), err)
		}

		inflight = append(inflight, abstract.ChangeItem{
			LSN:          lsn,
			CommitTime:   uint64(st.UnixNano()),
			Kind:         abstract.InsertKind,
			Schema:       table.Schema,
			Table:        table.Name,
			PartID:       "",
			ColumnNames:  colsNames,
			ColumnValues: columnValues,
			TableSchema:  tableSchema,
			ID:           0,
			Counter:      0,
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(readValuesSize),
		})
		globalIdx++
		if uint64(len(inflight)) >= chunkSize || inflightSize >= 16*size.MiB {
			if err := pusher(inflight); err != nil {
				return xerrors.Errorf("unable to push changes: %w", err)
			}
			inflight = make([]abstract.ChangeItem, 0)
			inflightSize = 0
		}
	}
	if uint64(len(inflight)) > 0 {
		if err := pusher(inflight); err != nil {
			return xerrors.Errorf("unable to push changes: %w", err)
		}
	}

	return nil
}

func beginROTransaction(s *Storage) (tx *sql.Tx, closeFunc func(), err error) {
	ctx := context.TODO()
	var conn *sql.Conn
	conn, err = s.DB.Conn(ctx)
	if err != nil {
		return nil, nil, xerrors.Errorf("Failed to open a read-only transaction: %w", err)
	}

	tx, err = conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if IsErrorCode(err, ErrCodeSyntax) {
		logger.Log.Warn("set readonly failed with `1064`-code, probably protocol mismatch, retry without it")
		// for single-store compatibility, see: https://docs.singlestore.com/cloud/reference/sql-reference/operational-commands/show-replication-status/
		tx, err = conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	}
	if err != nil {
		if err := conn.Close(); err != nil {
			logger.Log.Errorf("Failed to close connection: %v", err)
		}
		return nil, nil, xerrors.Errorf("Failed to open a read-only transaction: %w", err)
	}

	return tx, func() {
		if err := tx.Commit(); err != nil {
			logger.Log.Errorf("Failed to commit transaction: %v", err)
		}
		if err := conn.Close(); err != nil {
			logger.Log.Errorf("Failed to close connection: %v", err)
		}
	}, nil
}

func IsGtidModeEnabled(storage *Storage, flavor string) (bool, error) {
	if flavor == mysql.MariaDBFlavor {
		return true, nil
	}

	var gtidMode string
	if err := storage.DB.QueryRow("SELECT @@GLOBAL.GTID_MODE;").Scan(&gtidMode); err != nil {
		return false, xerrors.Errorf("Can't select gtid mode: %w", err)
	}
	return strings.ToLower(gtidMode) == "on", nil
}

func CheckMySQLVersion(storage *Storage) (string, string, error) {
	flavor := mysql.MySQLFlavor
	var version string

	var rows *sql.Rows
	err := backoff.RetryNotify(
		func() error {
			var err error
			rows, err = storage.DB.Query("SHOW VARIABLES LIKE '%version%';")
			return err
		},
		backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(time.Minute)),
		func(err error, d time.Duration) {
			msg := fmt.Sprintf("Unable to check version, will retry after %s", d.String())
			logger.Log.Warn(msg, log.Error(err))
		},
	)
	if err != nil {
		return "", "", xerrors.Errorf("unable to check version: %w", err)
	}

	defer rows.Close()
	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err != nil {
			return "", "", xerrors.Errorf("unable to parse version variable: %w", err)
		}
		if name == "version" {
			logger.Log.Infof("MySQL version is %v", val)
			val = strings.ToLower(val)
			if strings.Contains(strings.ToLower(val), "mariadb") {
				return mysql.MariaDBFlavor, val, nil
			}
			version = val
			break
		}
	}
	return flavor, version, nil
}
