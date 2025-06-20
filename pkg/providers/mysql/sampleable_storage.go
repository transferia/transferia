//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	row := s.DB.QueryRow(fmt.Sprintf("select data_length from information_schema.tables where table_name='%v'", table.Name))

	var size uint64
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	startTime := time.Now()

	tx, cf, err := beginROTransaction(s)
	if err != nil {
		return xerrors.Errorf("unable to begin read-only transaction: %w", err)
	}
	defer cf()

	schema, err := LoadSchema(tx, s.useFakePrimaryKey, true)
	if err != nil {
		return xerrors.Errorf("unable to load schema: %w", err)
	}

	rawSchema := schema[table.ID()]

	orderByPkeysAsc, err := OrderByPrimaryKeys(rawSchema.Columns(), "ASC")
	if err != nil {
		return xerrors.Errorf("Cannot build query for sampling table %s.%s: %w", table.Schema, table.Name, err)
	}
	orderByPkeysDesc, err := OrderByPrimaryKeys(rawSchema.Columns(), "DESC")
	if err != nil {
		return xerrors.Errorf("Cannot build query for sampling table %s.%s: %w", table.Schema, table.Name, err)
	}

	readQueryStart := buildSelectQuery(table, rawSchema.Columns()) + orderByPkeysAsc + " LIMIT 1000"
	readQueryEnd := buildSelectQuery(table, rawSchema.Columns()) + orderByPkeysDesc + " LIMIT 1000"
	totalQ := fmt.Sprintf(`
select * from (%v) as top
union
select * from (%v) as bot
`, readQueryStart, readQueryEnd)

	if err := s.loadSample(tx, totalQ, table, rawSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}

	return nil
}

func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	startTime := time.Now()

	tx, cf, err := beginROTransaction(s)
	if err != nil {
		return xerrors.Errorf("unable to begin read-only transaction: %w", err)
	}
	defer cf()

	schema, err := LoadSchema(tx, s.useFakePrimaryKey, true)
	if err != nil {
		return xerrors.Errorf("unable to load schema: %w", err)
	}

	rawSchema := schema[table.ID()]

	totalQ := buildSelectQuery(table, rawSchema.Columns()) + " WHERE RAND()<=0.05 LIMIT 2000"

	if err := s.loadSample(tx, totalQ, table, rawSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}

	return nil
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	startTime := time.Now()

	tx, cf, err := beginROTransaction(s)
	if err != nil {
		return xerrors.Errorf("unable to begin read-only transaction: %w", err)
	}
	defer cf()

	fqtnToSchema, err := LoadSchema(tx, s.useFakePrimaryKey, true)
	if err != nil {
		return xerrors.Errorf("unable to load schema: %w", err)
	}
	currTableSchema := fqtnToSchema[table.ID()]

	if _, err := tx.Exec("set net_read_timeout=3600;"); err != nil {
		return xerrors.Errorf("unable to set net read timeout: %w", err)
	}
	if _, err := tx.Exec("set net_write_timeout=3600;"); err != nil {
		return xerrors.Errorf("unable to set net write timeout: %w", err)
	}

	// TODO carefully on memory !
	var conditions []string
	for _, v := range keySet {
		var pkConditions []string
		for colName, val := range v {
			// string values won't work,
			// we need to represent from sink
			var columnType abstract.ColSchema
			for _, schemaElem := range currTableSchema.Columns() {
				if schemaElem.ColumnName == colName {
					columnType = schemaElem
					break
				}
			}
			pkConditions = append(pkConditions, fmt.Sprintf("`%v`=%v", colName, CastToMySQL(val, columnType)))
		}
		conditions = append(conditions, "("+strings.Join(pkConditions, " AND ")+")")
	}

	var totalCondition string
	if len(conditions) != 0 {
		totalCondition = " WHERE " + strings.Join(conditions, " OR ")
	} else {
		totalCondition = " WHERE FALSE"
	}

	totalQuerySelect := buildSelectQuery(table, currTableSchema.Columns()) + totalCondition

	if err := s.loadSample(tx, totalQuerySelect, table, currTableSchema, startTime, pusher); err != nil {
		return xerrors.Errorf("unable to load sample: %w", err)
	}

	return nil
}

func (s *Storage) loadSample(
	tx *sql.Tx,
	query string,
	table abstract.TableDescription,
	tableSchema *abstract.TableSchema,
	startTime time.Time,
	pusher abstract.Pusher,
) error {
	colNameToColTypeName, err := makeMapColNameToColTypeName(context.Background(), tx, table.Name)
	if err != nil {
		return xerrors.Errorf("unable to get column types: %w", err)
	}

	rows, err := tx.Query(query)
	if err != nil {
		msg := "error on performing select query"
		logger.Log.Errorf("%v: %v", msg, query)
		return xerrors.Errorf("%v: %w", msg, err)
	}
	defer rows.Close()

	logger.Log.Info("Sink uploading table", log.String("fqtn", table.Fqtn()))

	err = readRowsAndPushByChunks(
		s.ConnectionParams.Location,
		rows,
		startTime,
		table,
		tableSchema,
		colNameToColTypeName,
		2000,
		0,
		s.IsHomo,
		pusher,
	)
	if err != nil {
		return xerrors.Errorf("unable to read rows and push by chunks: %w", err)
	}

	if err := rows.Err(); err != nil {
		msg := "unable to read rows"
		logger.Log.Warn(msg, log.Error(err))
		return xerrors.Errorf("%v: %w", msg, err)
	}
	logger.Log.Info("Done read rows")
	return nil
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	row := s.DB.QueryRow(fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1;", table.Schema, table.Name))

	var check int
	if err := row.Scan(&check); err != nil && err != sql.ErrNoRows {
		logger.Log.Warnf("Inaccessible table %v: %v", table.Fqtn(), err)
		return false
	}
	return true
}
