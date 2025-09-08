package table_splitter

import (
	"context"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func SplitTables(
	ctx context.Context,
	logger log.Logger,
	source abstract.Storage,
	dstModel model.Destination,
	tables []abstract.TableDescription,
	tmpPolicyConfig *model.TmpPolicyConfig,
	operationID string,
) ([]*abstract.OperationTablePart, error) {
	var tablesParts []*abstract.OperationTablePart
	addTablesParts := func(shardedTable ...abstract.TableDescription) {
		for i, shard := range shardedTable {
			operationTable := abstract.NewOperationTablePartFromDescription(operationID, &shard)
			operationTable.PartsCount = uint64(len(shardedTable))
			operationTable.PartIndex = uint64(i)
			tablesParts = append(tablesParts, operationTable)
		}
	}

	shardingStorage, isShardingStorage := source.(abstract.ShardingStorage)
	isShardeableDestination := model.IsShardeableDestination(dstModel)
	isTmpPolicyEnabled := tmpPolicyConfig != nil

	reasonWhyNotSharded := ""
	if !isShardingStorage && !isShardeableDestination {
		reasonWhyNotSharded = "Source storage is not supported sharding table, and destination is not supported shardable snapshots - that's why tables won't be sharded here"
	} else if !isShardingStorage {
		reasonWhyNotSharded = "Source storage is not supported sharding table - that's why tables won't be sharded here"
	} else if !isShardeableDestination {
		reasonWhyNotSharded = "Destination is not supported shardable snapshots - that's why tables won't be sharded here"
	} else if isTmpPolicyEnabled {
		reasonWhyNotSharded = "Sharding is not supported by tmp policy, disabling it"
	}

	for _, table := range tables {
		if reasonWhyNotSharded == "" {
			tableParts, err := shardingStorage.ShardTable(ctx, table)
			if err != nil {
				if abstract.IsNonShardableError(err) {
					logger.Info("Unable to shard table", log.String("table", table.Fqtn()), log.Error(err))
					addTablesParts([]abstract.TableDescription{table}...)
				} else {
					return nil, xerrors.Errorf("unable to split table, err: %w", err)
				}
			}
			addTablesParts(tableParts...)
		} else {
			logger.Info(
				"table is not sharded (as all another tables)",
				log.String("table", table.Fqtn()),
				log.String("reason", reasonWhyNotSharded),
			)
			addTablesParts(table)
		}
	}

	// Big tables(or tables parts) go first;
	// This sort is same with sort on select from database;
	sort.Slice(tablesParts, func(i int, j int) bool {
		return util.Less(
			util.NewComparator(-tablesParts[i].ETARows, -tablesParts[j].ETARows),   // Sort desc
			util.NewComparator(tablesParts[i].Schema, tablesParts[j].Schema),       // Sort asc
			util.NewComparator(tablesParts[i].Name, tablesParts[j].Name),           // Sort asc
			util.NewComparator(tablesParts[i].PartIndex, tablesParts[j].PartIndex), // Sort asc
		)
	})

	return tablesParts, nil
}
