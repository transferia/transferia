//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"path"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

const defaultCopyFolder = "data-transfer"

func (s *Storage) modifyTableName(tablePath string) string {
	return strings.ReplaceAll(tablePath, "/", "_")
}

func (s *Storage) BeginSnapshot(ctx context.Context) error {
	if !s.config.IsSnapshotSharded {
		return nil
	}
	if s.config.CopyFolder == "" {
		s.config.CopyFolder = defaultCopyFolder
	}

	tables, err := s.listaAllTablesToTransfer(ctx)
	if err != nil {
		return xerrors.Errorf("Failed to list tables that will be transfered: %w", err)
	}

	if err := s.db.Scheme().MakeDirectory(ctx, s.makeTableDir()); err != nil {
		return xerrors.Errorf("failed to create copy directory: %w", err)
	}

	copyItems := make([]options.CopyTablesOption, len(tables))
	for i, tableName := range tables {
		tablePath := path.Join(s.config.Database, tableName)
		copyPath := s.makeTablePath("", s.modifyTableName(tableName))
		copyItems[i] = options.CopyTablesItem(tablePath, copyPath, false)
	}
	return s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		err = session.CopyTables(ctx, copyItems...)
		if err != nil {
			return xerrors.Errorf("failed to copy tables to transfer directory: %w", err)
		}
		return nil
	})
}

func (s *Storage) EndSnapshot(ctx context.Context) error {
	if !s.config.IsSnapshotSharded {
		return nil
	}

	copyDir := s.makeTableDir()
	content, err := s.db.Scheme().ListDirectory(ctx, copyDir)
	if err != nil {
		return xerrors.Errorf("failed to list copy directory: %w", err)
	}

	err = s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		for _, copyTable := range content.Children {
			copyPath := s.makeTablePath("", copyTable.Name)
			if copyTable.Type != scheme.EntryTable && copyTable.Type != scheme.EntryColumnTable {
				return xerrors.Errorf("only tables must be present in copy directory, found %v", copyPath)
			}
			if err = session.DropTable(ctx, copyPath); err != nil {
				return xerrors.Errorf("failed to drop copied table %v from transfer directory: %w", copyPath, err)
			}
		}
		return nil
	})
	if err != nil {
		return xerrors.Errorf("failed to drop copied tables: %w", err)
	}

	if err = s.db.Scheme().RemoveDirectory(ctx, copyDir); err != nil {
		return xerrors.Errorf("failed to remove copy directory: %w", err)
	}
	return nil
}

func (s *Storage) ShardTable(ctx context.Context, tableDesc abstract.TableDescription) ([]abstract.TableDescription, error) {
	if !s.config.IsSnapshotSharded {
		return []abstract.TableDescription{tableDesc}, nil
	}

	copyPath := s.makeTablePath(tableDesc.Schema, tableDesc.Name)
	var result []abstract.TableDescription
	err := s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		tableDescription, err := session.DescribeTable(ctx, copyPath, options.WithShardKeyBounds())
		if err != nil {
			return xerrors.Errorf("unable to describe table: %w", err)
		}

		result = make([]abstract.TableDescription, len(tableDescription.KeyRanges))
		for i := range tableDescription.KeyRanges {
			result[i] = tableDesc
			result[i].Offset = uint64(i)
		}
		return nil
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to schard table %v : %w", copyPath, err)
	}

	return result, nil
}

func (s *Storage) makeTablePath(schema, name string) string {
	tableDir := s.makeTableDir()
	if !s.config.IsSnapshotSharded {
		return path.Join(tableDir, schema, name)
	}
	return path.Join(tableDir, schema, s.modifyTableName(name))
}

func (s *Storage) makeTableDir() string {
	if !s.config.IsSnapshotSharded {
		return s.config.Database
	}
	return path.Join(s.config.Database, s.config.CopyFolder)
}
