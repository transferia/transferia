//go:build !disable_opensearch_provider

package opensearch

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/elastic"
	"go.ytsaurus.tech/library/go/core/log"
)

type Storage struct {
	elasticStorage         abstract.Storage
	elasticShardingStorage abstract.ShardingStorage
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return s.elasticStorage.Ping()
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.elasticStorage.EstimateTableRowsCount(table)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.elasticStorage.ExactTableRowsCount(table)
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return s.elasticStorage.LoadTable(ctx, table, pusher)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.elasticStorage.TableExists(table)
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.elasticStorage.TableSchema(ctx, table)
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.elasticStorage.TableList(includeTableFilter)
}

func NewStorage(src *OpenSearchSource, logger log.Logger, mRegistry metrics.Registry, opts ...elastic.StorageOpt) (*Storage, error) {
	elasticSrc, serverType := src.ToElasticSearchSource()
	eStorage, err := elastic.NewStorage(elasticSrc, logger, mRegistry, serverType, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic storage: %w", err)
	}

	return &Storage{
		elasticStorage:         eStorage,
		elasticShardingStorage: eStorage,
	}, nil
}
