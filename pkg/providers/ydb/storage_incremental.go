//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/predicate"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var _ abstract.IncrementalStorage = (*Storage)(nil)

func (s *Storage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	res := make([]abstract.IncrementalState, 0, len(incremental))
	for _, tbl := range incremental {
		fullPath := path.Join(s.config.Database, tbl.Name)
		ydbTableDesc, err := describeTable(ctx, s.db, fullPath, options.WithShardKeyBounds())
		if err != nil {
			return nil, xerrors.Errorf("error describing table %s: %w", tbl.Name, err)
		}
		if len(ydbTableDesc.PrimaryKey) != 1 || ydbTableDesc.PrimaryKey[0] != tbl.CursorField {
			return nil, xerrors.Errorf("only primary key may be used as cursor field for YDB incremental snapshot (table `%s` has key `%s`, not `%s`)",
				tbl.Name, strings.Join(ydbTableDesc.PrimaryKey, ", "), tbl.CursorField)
		}
		val, err := s.getMaxKeyValue(ctx, tbl.Name, ydbTableDesc)
		if err != nil {
			return nil, xerrors.Errorf("error getting max key value for table %s, key %s: %w", tbl.Name, tbl.CursorField, err)
		}
		name := tbl.Name
		res = append(res, abstract.IncrementalState{
			Name:    name,
			Schema:  "",
			Payload: abstract.WhereStatement(fmt.Sprintf(`"%s" > %s`, tbl.CursorField, strconv.Quote(val.Yql()))),
		})
	}

	return res, nil
}

func (s *Storage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incremental []abstract.IncrementalTable) []abstract.TableDescription {
	result := slices.Clone(tables)
	incrementMap := make(map[abstract.TableID]abstract.IncrementalTable, len(incremental))
	for _, t := range incremental {
		incrementMap[t.TableID()] = t
	}
	for i := range result {
		if result[i].Filter != "" {
			continue
		}
		incr, ok := incrementMap[result[i].ID()]
		if !ok || incr.InitialState == "" || incr.CursorField == "" {
			continue
		}
		result[i].Filter = abstract.WhereStatement(fmt.Sprintf(`"%s" > %s`, incr.CursorField, strconv.Quote(incr.InitialState)))
	}
	return result
}

func (s *Storage) getMaxKeyValue(ctx context.Context, path string, tbl *options.Description) (types.Value, error) {
	if l := len(tbl.PrimaryKey); l != 1 {
		return nil, xerrors.Errorf("unexpected primary key length %d", l)
	}
	keyCol := tbl.PrimaryKey[0]

	keyColDesc := yslices.Filter(tbl.Columns, func(col options.Column) bool {
		return col.Name == keyCol
	})
	if l := len(keyColDesc); l != 1 {
		return nil, xerrors.Errorf("found unexpected count of key column description: %d", l)
	}

	maxPartitionKey := tbl.KeyRanges[len(tbl.KeyRanges)-1].From

	var queryParams *table.QueryParameters
	query := fmt.Sprintf("--!syntax_v1\nSELECT `%[2]s` FROM `%[1]s` ORDER BY `%[2]s` DESC LIMIT 1",
		path, keyCol)

	// YDB lookup all partitions to find max key and this may take a long time
	// We may hint it to look only to the partition containing the highest key values
	if maxPartitionKey != nil {
		query = fmt.Sprintf("--!syntax_v1\nDECLARE $keyValue as %[3]s;\nSELECT `%[2]s` FROM `%[1]s` WHERE `%[2]s` >= $keyValue ORDER BY `%[2]s` DESC LIMIT 1",
			path, keyCol, keyColDesc[0].Type.Yql())
		queryParams = table.NewQueryParameters(table.ValueParam("keyValue", maxPartitionKey))
	}

	return s.querySingleValue(ctx, query, keyCol, queryParams)
}

func parseKeyFilter(expr abstract.WhereStatement, colName string) (fromKeyStr, toKeyStr string, err error) {
	// Supported filter forms are:
	// - NOT ("key" > val) - increments with empty initial state
	// - ("key" > val) AND (NOT ("key" > val))
	parts, err := predicate.InclusionOperands(expr, colName)
	if err != nil {
		return "", "", err
	}

	partsCnt := len(parts)
	if partsCnt < 1 || partsCnt > 2 {
		return "", "", xerrors.Errorf("key filter must consists of 1 or 2 parts, not %d", partsCnt)
	}

	toExpr := parts[0]
	if partsCnt == 2 {
		toExpr = parts[1]
	}

	toVal, err := extractParsedValue(toExpr)
	if err != nil {
		return "", "", xerrors.Errorf("error extracting upper filter bound: %w", err)
	}
	if partsCnt == 1 {
		return "", toVal, nil
	}

	fromVal, err := extractParsedValue(parts[0])
	if err != nil {
		return "", "", xerrors.Errorf("error extracting lower filter bound: %w", err)
	}
	return fromVal, toVal, nil
}

func extractParsedValue(op predicate.Operand) (string, error) {
	v, ok := op.Val.(string)
	if !ok {
		return "", xerrors.Errorf("expected string, not %T", v)
	}
	// pkg/predicate drops wrapping quotes from string
	// so resulting string should be wrapped again to be unquoted properly
	// see https://st.yandex-team.ru/TM-6741
	v, err := strconv.Unquote("\"" + v + "\"")
	if err != nil {
		return "", xerrors.Errorf("unquoting error: %w", err)
	}
	return v, nil
}

func (s *Storage) resolveExprValue(ctx context.Context, yqlStr string, typ types.Type) (val types.Value, err error) {
	query := fmt.Sprintf("--!syntax_v1\nSELECT CAST(%[1]s AS %[2]s) AS `val`", yqlStr, typ.Yql())
	return s.querySingleValue(ctx, query, "val", nil)
}

func (s *Storage) filterToKeyRange(ctx context.Context, filter abstract.WhereStatement, ydbTable options.Description) (keyTupleFrom, keyTupleTo types.Value, err error) {
	keyColName := ydbTable.PrimaryKey[0]
	keyCol := yslices.Filter(ydbTable.Columns, func(col options.Column) bool {
		return col.Name == keyColName
	})[0]

	fromStr, toStr, err := parseKeyFilter(filter, keyColName)
	if err != nil {
		return nil, nil, xerrors.Errorf("error parsing filter %s: %w", filter, err)
	}

	toVal, err := s.resolveExprValue(ctx, toStr, keyCol.Type)
	if err != nil {
		return nil, nil, xerrors.Errorf("error resolving upper key %s to YDB value: %w", toStr, err)
	}
	toVal = types.TupleValue(toVal)
	if fromStr == "" {
		return nil, toVal, nil
	}

	fromVal, err := s.resolveExprValue(ctx, fromStr, keyCol.Type)
	if err != nil {
		return nil, nil, xerrors.Errorf("error resolving lower key %s to YDB value: %w", fromStr, err)
	}
	return types.TupleValue(fromVal), toVal, nil
}

func (s *Storage) querySingleValue(ctx context.Context, query string, colName string, params *table.QueryParameters) (types.Value, error) {
	var val types.Value
	hasRows := false
	err := s.db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, query, params)
		if err != nil {
			return xerrors.Errorf("error executing single value query: %w", err)
		}
		defer res.Close()
		for res.NextResultSet(ctx, colName) {
			for res.NextRow() {
				if err := res.Scan(&val); err != nil {
					return xerrors.Errorf("scan error: %w", err)
				}
				hasRows = true
			}
		}
		return res.Err()
	})
	if err != nil {
		return nil, err
	}
	if !hasRows {
		return nil, sql.ErrNoRows
	}
	return val, nil
}
