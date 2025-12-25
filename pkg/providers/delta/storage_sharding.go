package delta

import (
	"context"
	"fmt"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ abstract.ShardingStorage        = (*Storage)(nil)
	_ abstract.ShardingContextStorage = (*Storage)(nil)
)

func (s *Storage) ShardTable(_ context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Filter != "" || table.Offset != 0 {
		logger.Log.Infof("Table %v will not be sharded, filter: [%v], offset: %v", table.Fqtn(), table.Filter, table.Offset)
		return []abstract.TableDescription{table}, nil
	}
	if err := s.ensureSnapshot(); err != nil {
		return nil, xerrors.Errorf("unable to ensure snapshot: %w", err)
	}
	files, err := s.snapshot.AllFiles()
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}
	var res []abstract.TableDescription
	for _, file := range files {
		res = append(res, abstract.TableDescription{
			Name:   s.cfg.TableName,
			Schema: s.cfg.TableNamespace,
			Filter: abstract.WhereStatement(file.Path),
			EtaRow: 0,
			Offset: 0,
		})
	}
	return res, nil
}

func (s *Storage) ShardingContext() ([]byte, error) {
	if err := s.ensureSnapshot(); err != nil {
		return nil, xerrors.Errorf("unable to ensure snapshot for sharding context: %w", err)
	}
	return []byte(fmt.Sprintf("%v", s.snapshot.CommitTS().UnixMilli())), nil
}

func (s *Storage) SetShardingContext(shardedState []byte) error {
	var err error
	s.snapshot, err = s.table.SnapshotForTimestamp(cast.ToInt64(shardedState))
	if err != nil {
		return xerrors.Errorf("unable to set snapshot for ts: %v: %w", cast.ToInt64(shardedState), err)
	}
	meta, err := s.snapshot.Metadata()
	if err != nil {
		return xerrors.Errorf("unable to load meta: %w", err)
	}
	typ, err := meta.DataSchema()
	if err != nil {
		return xerrors.Errorf("unable to load data scheam: %w", err)
	}
	s.tableSchema = s.asTableSchema(typ)
	s.colNames = yslices.Map(s.tableSchema.Columns(), func(t abstract.ColSchema) string {
		return t.ColumnName
	})
	return nil
}
