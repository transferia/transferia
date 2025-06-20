//go:build !disable_opensearch_provider

package opensearch

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	return s.elasticShardingStorage.ShardTable(ctx, table)
}
