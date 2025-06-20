//go:build !disable_clickhouse_provider

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

const (
	getTableSize            = "SELECT sum(bytes) as size FROM system.parts WHERE active AND `database`=? AND `table`=? GROUP BY table;"
	checkTableAccessibility = "SELECT 1 FROM %s LIMIT 1;"
	queryExactRows          = "SELECT COUNT(1) FROM %s %s WHERE 1=1 %s"
)

func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	sizeRow := s.db.QueryRow(getTableSize, table.Namespace, table.Name)
	var size uint64
	if err := sizeRow.Scan(&size); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, xerrors.Errorf("error while getting TableSizeInBytes of table %s : %w", table.Fqtn(), err)
	}
	return size, nil
}

func (s *Storage) readRowsAndPushByChunksWrapped(table abstract.TableDescription, schema *abstract.TableSchema, query string, pusher abstract.Pusher) error {
	backgroundContext := context.Background()
	conn, err := s.db.Conn(backgroundContext)
	if err != nil {
		return xerrors.Errorf("unable to get connection: %w", err)
	}
	rows, err := conn.QueryContext(backgroundContext, query)
	if err != nil {
		return xerrors.Errorf("unable to query: %w", err)
	}

	reader := &rowsReader{
		table:                table,
		rows:                 rows,
		ts:                   0,
		chunkSize:            1000,
		tableAllColumns:      schema,
		tableFilteredColumns: schema,
		inflightBytesLimit:   s.bufSize,
	}
	if err := s.readRowsAndPushByChunks(reader, pusher, table); err != nil {
		return xerrors.Errorf("unable to upload table %v: %w", table.Fqtn(), err)
	}
	return nil
}

func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	rawSchema, _, err := s.getTableSchema(table.ID(), true)
	if err != nil {
		return xerrors.Errorf("Cannot get table schema for sampling table %s.%s: %w", table.Schema, table.Name, err)
	}
	deletable, err := s.isDeletable(table.ID())
	if err != nil {
		return xerrors.Errorf("failed to determine deletable table %s: %w", table.Fqtn(), err)
	}
	orderByPkeysAsc, err := orderByPrimaryKeys(rawSchema.Columns(), "ASC")
	if err != nil {
		return xerrors.Errorf("Cannot build query for sampling table %s.%s: %w", table.Schema, table.Name, err)
	}
	orderByPkeysDesc, err := orderByPrimaryKeys(rawSchema.Columns(), "DESC")
	if err != nil {
		return xerrors.Errorf("Cannot build query for sampling table %s.%s: %w", table.Schema, table.Name, err)
	}

	readQueryStart := buildSelectQuery(&table, rawSchema.Columns(), true, deletable, "") + orderByPkeysAsc + " LIMIT 1000"
	readQueryEnd := buildSelectQuery(&table, rawSchema.Columns(), true, deletable, "") + orderByPkeysDesc + " LIMIT 1000"
	totalQ := fmt.Sprintf(`%s UNION ALL %s`, readQueryStart, readQueryEnd)

	return s.readRowsAndPushByChunksWrapped(table, rawSchema, totalQ, pusher)
}

func orderByPrimaryKeys(tableSchema []abstract.ColSchema, direction string) (string, error) {
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

func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	rawSchema, _, err := s.getTableSchema(table.ID(), true)
	if err != nil {
		return xerrors.Errorf("unable to get table schema: %w", err)
	}
	deletable, err := s.isDeletable(table.ID())
	if err != nil {
		return xerrors.Errorf("failed to determine updateable table %s: %w", table.Fqtn(), err)
	}

	readQuery := buildSelectQuery(&table, rawSchema.Columns(), true, deletable, "(rand() / 4294967296)<=0.05") + " LIMIT 2000"
	return s.readRowsAndPushByChunksWrapped(table, rawSchema, readQuery, pusher)
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	rawSchema, _, err := s.getTableSchema(table.ID(), true)
	if err != nil {
		return xerrors.Errorf("unable to get table schema: %w", err)
	}
	deletable, err := s.isDeletable(table.ID())
	if err != nil {
		return xerrors.Errorf("failed to determine updateable table %s: %w", table.Fqtn(), err)
	}
	colNameToIdx := make(map[string]int)
	for i, col := range rawSchema.Columns() {
		colNameToIdx[col.ColumnName] = i
	}

	conditions := make([]string, 0)
	condValues := make([]interface{}, 0)
	for _, v := range keySet {
		var pkConditions []string
		for colName, val := range v {
			idx, ok := colNameToIdx[colName]
			if !ok {
				return xerrors.Errorf("unknown column name: %s", colName)
			}

			pkConditions = append(pkConditions, colName+"=?")
			condValues = append(condValues, columntypes.Restore(rawSchema.Columns()[idx], val))
		}
		conditions = append(conditions, "("+strings.Join(pkConditions, " AND ")+")")
	}

	condStr := strings.Join(conditions, " OR ")
	queryTemplate := buildSelectQuery(&table, rawSchema.Columns(), true, deletable, condStr)

	selectQuery, err := s.db.Prepare(queryTemplate)
	if err != nil {
		return xerrors.Errorf("unable to prepare query %s: %w", queryTemplate, err)
	}

	readQuery, err := selectQuery.Query(condValues...)
	if err != nil {
		return xerrors.Errorf("unable to select query %s with values %v: %w", queryTemplate, condValues, err)
	}
	defer readQuery.Close()

	reader := &rowsReader{
		table:                table,
		rows:                 readQuery,
		ts:                   0,
		chunkSize:            1000,
		tableAllColumns:      rawSchema,
		tableFilteredColumns: rawSchema,
		inflightBytesLimit:   s.bufSize,
	}
	return s.readRowsAndPushByChunks(reader, pusher, table)
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	row := s.db.QueryRow(fmt.Sprintf(checkTableAccessibility, table.Fqtn()))

	var check int
	if err := row.Scan(&check); err != nil && err != sql.ErrNoRows {
		s.logger.Warnf("Inaccessible table %v: %v", table.Fqtn(), err)
		return false
	}
	return true
}
