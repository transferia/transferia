package fake_sharding_storage

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type FakeShardingStorage struct {
	tables []abstract.TableDescription
}

func (f *FakeShardingStorage) TableSchema(_ context.Context, _ abstract.TableID) (*abstract.TableSchema, error) {
	return nil, nil
}

func (f *FakeShardingStorage) Close() {
}

func (f *FakeShardingStorage) Ping() error {
	return nil
}

func (f *FakeShardingStorage) LoadTable(_ context.Context, _ abstract.TableDescription, _ abstract.Pusher) error {
	return nil
}

func (f *FakeShardingStorage) TableList(abstract.IncludeTableList) (abstract.TableMap, error) {
	return nil, nil
}

func (f *FakeShardingStorage) ShardTable(_ context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Offset != 0 {
		logger.Log.Infof("Table %v will not be sharded, offset: %v", table.Fqtn(), table.Offset)
		return []abstract.TableDescription{table}, nil
	}

	var res []abstract.TableDescription
	for i := 0; i < 10; i++ {
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("shard = '%v'", i)),
		})
	}
	return res, nil
}

func (f *FakeShardingStorage) ExactTableRowsCount(_ abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (f *FakeShardingStorage) EstimateTableRowsCount(_ abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (f *FakeShardingStorage) TableExists(_ abstract.TableID) (bool, error) {
	return false, xerrors.New("not implemented")
}

func NewFakeShardingStorage(tables []abstract.TableDescription) *FakeShardingStorage {
	return &FakeShardingStorage{
		tables: tables,
	}
}
