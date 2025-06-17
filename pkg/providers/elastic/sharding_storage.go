//go:build !disable_elastic_provider

package elastic

import (
	"context"
	"encoding/json"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

var _ abstract.ShardingStorage = (*Storage)(nil)

type ShardingFilter struct {
	ID  int `json:"id"`
	Max int `json:"max"`
}

var emptyFilter = ShardingFilter{
	ID:  0,
	Max: 0,
}

func UnmarshalFilter(marshalledFilter string) (ShardingFilter, error) {
	var filter ShardingFilter

	err := jsonx.Unmarshal([]byte(marshalledFilter), &filter)
	if err != nil {
		return ShardingFilter{}, xerrors.Errorf("cannot unmarshal filter: %w", err)
	}
	return filter, nil
}

func filterFromTable(table abstract.TableDescription) (ShardingFilter, error) {
	filter := ShardingFilter(emptyFilter)

	if table.Filter != "" {
		var err error
		filter, err = UnmarshalFilter(string(table.Filter))
		if err != nil {
			return ShardingFilter{}, xerrors.Errorf("cannot unmarshal filter from table description: %w", err)
		}
	}
	return filter, nil
}

// Fetch amount of active shards for index in order to calculate ideal slicing for parallelized execution
// https://www.elastic.co/guide/en/elasticsearch/reference/master/paginate-search-results.html#slice-scroll sliceNr  <= shardsNr
func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Filter != "" || table.Offset != 0 {
		logger.Log.Infof("Table %v will not be sharded, filter: [%v], offset: %v", table.Fqtn(), table.Filter, table.Offset)
		return []abstract.TableDescription{table}, nil
	}

	exist, err := s.TableExists(table.ID())
	if err != nil || !exist {
		return nil, xerrors.Errorf("could not find table to shard: %s, err: %w", table.Name, err)
	}

	body, err := getResponseBody(s.Client.Cluster.Health(s.Client.Cluster.Health.WithIndex(table.Name)))
	if err != nil {
		return nil, xerrors.Errorf("could not fetch cluster information: %s, err: %w", table.Name, err)
	}

	var healthResponse healthResponse
	if err := jsonx.Unmarshal(body, &healthResponse); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal healthResponse, index: %s, err: %w", table.Name, err)
	}

	result := []abstract.TableDescription{}

	if healthResponse.Shards == 1 {
		// only one shard, defaulting to simple scroll
		return []abstract.TableDescription{table}, nil
	} else {
		for searchIndex := 0; searchIndex < healthResponse.Shards; searchIndex++ {
			filter := ShardingFilter{
				ID:  searchIndex,
				Max: healthResponse.Shards,
			}

			marshaledFilter, err := json.Marshal(filter)
			if err != nil {
				return nil, xerrors.Errorf("cannot marshal filter: %w", err)
			}
			result = append(result, abstract.TableDescription{
				Name:   table.Name,
				Schema: table.Schema,
				Filter: abstract.WhereStatement(marshaledFilter),
				EtaRow: 0,
				Offset: 0,
			})
		}
	}

	return result, nil
}
