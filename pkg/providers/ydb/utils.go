//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"path"
	"regexp"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func filterYdbTableColumns(filter []YdbColumnsFilter, description options.Description) ([]options.Column, error) {
	for _, filterRule := range filter {
		tablesWithFilterRegExp, err := regexp.Compile(filterRule.TableNamesRegexp)
		if err != nil {
			return nil, xerrors.Errorf("unable to compile regexp: %s: %w", filterRule.TableNamesRegexp, err)
		}
		if !tablesWithFilterRegExp.MatchString(description.Name) {
			continue
		}
		primaryKey := map[string]bool{}
		for _, k := range description.PrimaryKey {
			primaryKey[k] = true
		}
		columnsToFilterRegExp, err := regexp.Compile(filterRule.ColumnNamesRegexp)
		if err != nil {
			return nil, xerrors.Errorf("unable to compile regexp: %s: %w", filterRule.ColumnNamesRegexp, err)
		}
		filteredColumns := make([]options.Column, 0)
		for _, column := range description.Columns {
			hasMatch := columnsToFilterRegExp.MatchString(column.Name)
			if (filterRule.Type == YdbColumnsWhiteList && hasMatch) ||
				(filterRule.Type == YdbColumnsBlackList && !hasMatch) {
				filteredColumns = append(filteredColumns, column)
			} else {
				if primaryKey[column.Name] {
					errorMessage := "Table loading failed. Unable to filter primary key %s of table: %s"
					return nil, xerrors.Errorf(errorMessage, column.Name, description.Name)
				}
			}
		}
		if len(filteredColumns) == 0 {
			errorMessage := "Table loading failed. Got empty list of columns after filtering: %s"
			return nil, xerrors.Errorf(errorMessage, description.Name)
		}
		return filteredColumns, nil
	}
	return description.Columns, nil
}

func tableSchema(ctx context.Context, db *ydb.Driver, database string, tableID abstract.TableID) (*abstract.TableSchema, error) {
	tablePath := path.Join(database, tableID.Namespace, tableID.Name)
	desc, err := describeTable(ctx, db, tablePath)
	if err != nil {
		return nil, err
	}
	return abstract.NewTableSchema(FromYdbSchema(desc.Columns, desc.PrimaryKey)), nil
}

func describeTable(ctx context.Context, db *ydb.Driver, tablePath string, opts ...options.DescribeTableOption) (*options.Description, error) {
	var desc options.Description
	err := db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err = session.DescribeTable(ctx, tablePath, opts...)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &desc, nil
}
