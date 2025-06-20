//go:build !disable_yt_provider

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ytprovider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var size uint64
	if err := s.ytClient.GetNode(ctx, ytprovider.SafeChild(ypath.Path(s.path), table.Name).Attr("uncompressed_data_size"), &size, nil); err != nil {
		return 0, err
	}

	return size, nil
}

func buildSelectQuery(table abstract.TableDescription, tablePath ypath.Path) string {
	resultQuery := fmt.Sprintf(
		"* FROM [%v] ",
		tablePath,
	)

	if table.Filter != "" {
		resultQuery += " WHERE " + string(table.Filter)
	}
	if table.Offset != 0 {
		resultQuery += fmt.Sprintf(" OFFSET %d", table.Offset)
	}

	return resultQuery
}

func orderByPrimaryKeys(tableSchema []abstract.ColSchema, direction string) (string, error) {
	var keys []string
	for _, col := range tableSchema {
		if col.PrimaryKey {
			keys = append(keys, fmt.Sprintf("%s %s", col.ColumnName, direction))
		}
	}
	if len(keys) == 0 {
		return "", xerrors.New("No key columns found")
	}
	return " ORDER BY " + strings.Join(keys, ","), nil
}

//nolint:descriptiveerrors
func pushChanges(
	ctx context.Context,
	pusher abstract.Pusher,
	reader yt.TableReader,
	tableSchema *abstract.TableSchema,
	table abstract.TableDescription,
) error {
	cols := make([]string, len(tableSchema.Columns()))
	for i, c := range tableSchema.Columns() {
		cols[i] = c.ColumnName
	}

	st, err := util.GetTimestampFromContext(ctx)
	if err != nil || st.IsZero() {
		st = time.Now()
	}

	rIdx := 0
	cIdx := 0

	changes := make([]abstract.ChangeItem, 0)
	for reader.Next() {
		vals := make([]interface{}, len(cols))

		r := map[string]interface{}{}
		if err := reader.Scan(&r); err != nil {
			return err
		}
		for i, colName := range cols {
			vals[i] = r[colName]
		}
		changes = append(changes, abstract.ChangeItem{
			CommitTime:   uint64(st.UnixNano()),
			Kind:         abstract.InsertKind,
			Table:        table.Name,
			ColumnNames:  cols,
			ColumnValues: vals,
			TableSchema:  tableSchema,
			PartID:       "",
			ID:           0,
			LSN:          0,
			Counter:      0,
			Schema:       "",
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(util.DeepSizeof(r)),
		})
		if cIdx == 10000 {
			if err := pusher(changes); err != nil {
				return err
			}
			changes = make([]abstract.ChangeItem, 0)
			cIdx = 0
		}
		rIdx++
		cIdx++
	}

	if cIdx != 0 {
		if err := pusher(changes); err != nil {
			return err
		}
	}

	return nil
}

//nolint:descriptiveerrors
func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tablePath := ytprovider.SafeChild(ypath.Path(s.path), getTableName(table))

	var scheme schema.Schema
	if err := s.ytClient.GetNode(ctx, tablePath.Attr("schema"), &scheme, nil); err != nil {
		return err
	}

	tableSchema := ytprovider.YTColumnToColSchema(scheme.Columns)

	orderByPkeysAsc, err := orderByPrimaryKeys(tableSchema.Columns(), "ASC")
	if err != nil {
		return xerrors.Errorf("Table %s.%s: %w", table.Schema, table.Name, err)
	}
	orderByPkeysDesc, err := orderByPrimaryKeys(tableSchema.Columns(), "DESC")
	if err != nil {
		return xerrors.Errorf("Table %s.%s: %w", table.Schema, table.Name, err)
	}

	queryStart := buildSelectQuery(table, tablePath) + orderByPkeysAsc + " LIMIT 1000"
	queryEnd := buildSelectQuery(table, tablePath) + orderByPkeysDesc + " LIMIT 1000"

	readerStart, err := s.ytClient.SelectRows(
		ctx,
		queryStart,
		&yt.SelectRowsOptions{},
	)
	if err != nil {
		return err
	}

	readerEnd, err := s.ytClient.SelectRows(
		ctx,
		queryEnd,
		&yt.SelectRowsOptions{},
	)
	if err != nil {
		return err
	}

	err = pushChanges(ctx, pusher, readerStart, tableSchema, table)
	if err != nil {
		return err
	}

	err = pushChanges(ctx, pusher, readerEnd, tableSchema, table)
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tablePath := ytprovider.SafeChild(ypath.Path(s.path), getTableName(table))

	var scheme schema.Schema
	if err := s.ytClient.GetNode(ctx, tablePath.Attr("schema"), &scheme, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}

	tableSchema := ytprovider.YTColumnToColSchema(scheme.Columns)

	var cols []string
	for _, col := range tableSchema.Columns() {
		if col.PrimaryKey {
			cols = append(cols, col.ColumnName)
		}
	}
	if len(cols) == 0 {
		return xerrors.Errorf("No key columns found for table %s.%s", table.Schema, table.Name)
	}

	totalQuerySelect := buildSelectQuery(table, tablePath) + " WHERE farm_hash(" + strings.Join(cols, ", ") + ")%20=0 LIMIT 1000"

	reader, err := s.ytClient.SelectRows(
		ctx,
		totalQuerySelect,
		&yt.SelectRowsOptions{},
	)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	err = pushChanges(ctx, pusher, reader, tableSchema, table)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	return nil
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tablePath := ytprovider.SafeChild(ypath.Path(s.path), getTableName(table))

	var scheme schema.Schema
	if err := s.ytClient.GetNode(ctx, tablePath.Attr("schema"), &scheme, nil); err != nil {
		return err
	}

	tableSchema := ytprovider.YTColumnToColSchema(scheme.Columns)

	var conditions []string
	for _, v := range keySet {
		var pkConditions []string
		for colName, val := range v {
			pkConditions = append(pkConditions, fmt.Sprintf("`%v`=%v", colName, val))
		}
		conditions = append(conditions, "("+strings.Join(pkConditions, " AND ")+")")
	}

	var totalCondition string
	if len(conditions) != 0 {
		totalCondition = " WHERE " + strings.Join(conditions, " OR ")
	} else {
		totalCondition = " WHERE FALSE"
	}

	totalQuerySelect := buildSelectQuery(table, tablePath) + totalCondition

	reader, err := s.ytClient.SelectRows(
		ctx,
		totalQuerySelect,
		&yt.SelectRowsOptions{},
	)
	if err != nil {
		return xerrors.Errorf("unable to select: %s: %w", totalQuerySelect, err)
	}

	err = pushChanges(ctx, pusher, reader, tableSchema, table)
	if err != nil {
		return xerrors.Errorf("unable to push changes: %w", err)
	}

	return nil
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dummyS := struct{}{}

	if err := s.ytClient.GetNode(ctx, ytprovider.SafeChild(ypath.Path(s.path), getTableName(table)), dummyS, nil); err != nil {
		logger.Log.Warnf("Inaccessible table %v: %v", table.Fqtn(), err)
		return false
	}

	return true
}
