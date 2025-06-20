//go:build !disable_delta_provider

package delta

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
)

// To verify providers contract implementation
var (
	_ abstract.SnapshotableStorage = (*Storage)(nil)
)

func (s *Storage) ensureSnapshot() error {
	if s.snapshot == nil {
		snapshot, err := s.table.Snapshot()
		if err != nil {
			return xerrors.Errorf("unable to build a snapshot: %w", err)
		}
		s.logger.Infof("init snapshot at version: %v for timestamp: %v", snapshot.Version(), snapshot.CommitTS())
		s.snapshot = snapshot
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
	}
	return nil
}

func (s *Storage) BeginSnapshot(_ context.Context) error {
	return s.ensureSnapshot()
}

func (s *Storage) EndSnapshot(_ context.Context) error {
	s.snapshot = nil
	return nil
}
