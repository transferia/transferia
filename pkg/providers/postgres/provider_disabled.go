//go:build disable_postgres_provider

package postgres

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Storage struct{}

func (s *Storage) Close() {}

func (s *Storage) Ping() error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return nil, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) LoadTableImplNonDistributed(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) LoadTableImplDistributed(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return nil, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return false, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	return nil, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) ShardingContext() ([]byte, error) {
	return nil, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) SetShardingContext(shardedState []byte) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) BeginGPSnapshot(ctx context.Context, tables []abstract.TableDescription) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) EndGPSnapshot(ctx context.Context) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) WorkersCount() int {
	return 0
}

func (s *Storage) SetWorkersCount(count int) {}

func (s *Storage) RunSlotMonitor(ctx context.Context, serverSource interface{}, registry metrics.Registry) (abstract.SlotKiller, <-chan error, error) {
	return nil, nil, xerrors.New("Postgres is not available in this build")
}

func (s *Storage) BeginPGSnapshot(ctx context.Context) error {
	return xerrors.New("Postgres is not available in this build")
}

func (s *Storage) SnapshotLSN() string {
	return ""
}
