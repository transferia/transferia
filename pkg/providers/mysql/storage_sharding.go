//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	partitions, err := s.resolvePartitions(ctx, table)
	if err != nil {
		logger.Log.Warnf("unable to load child tables: %v", err)
	}
	if len(partitions) > 0 {
		return partitions, nil
	}

	return []abstract.TableDescription{table}, nil
}

func (s *Storage) resolvePartitions(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		`
SELECT PARTITION_NAME, TABLE_ROWS
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve partitions: %w", err)
	}
	defer rows.Close()
	var res []abstract.TableDescription
	for rows.Next() {
		var partName string
		var tableRows int
		if err := rows.Scan(&partName, &tableRows); err != nil {
			return nil, xerrors.Errorf("unable to scan partition: %w", err)
		}
		logger.Log.Infof("resolve part: %s (%v)", partName, tableRows)
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("PARTITION (%s)", partName)),
			EtaRow: uint64(tableRows),
			Offset: 0,
		})
	}
	if rows.Err() != nil {
		return nil, xerrors.Errorf("unable to read rows: %w", rows.Err())
	}
	return res, nil
}
