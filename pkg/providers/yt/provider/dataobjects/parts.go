package dataobjects

import (
	"context"
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	xslices "golang.org/x/exp/slices"
)

// grpcShardLimit caps the total number of parts a single ShardTable call may
// emit.
const grpcShardLimit = 1024

var tablesWeightOverflowErr = xerrors.NewSentinel("total tables weight overflow")

// CheckTableCountLimit returns an error when the snapshot contains more than
// grpcShardLimit tables. Enforced on every listing of the row source, matching
// the legacy uniformParts guard that ran on every abstract2 snapshot
// regardless of sharding.
func CheckTableCountLimit(tables cypressmeta.YtNodes) error {
	if len(tables) > grpcShardLimit {
		return xerrors.Errorf("%v tables. Can not be more than 1024 tables", len(tables))
	}
	return nil
}

// ComputeParts splits a batch of tables into SDK table partitions.
func ComputeParts(
	ctx context.Context,
	tx yt.Tx,
	txID yt.TxID,
	tables cypressmeta.YtNodes,
	cfg provider_yt.YtSourceModel,
	lgr log.Logger,
	columns map[yt.NodeID][]string,
) ([]abstract.TableDescription, error) {
	partsMapping, err := ComputePartsMapping(tables, cfg, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to compute parts mapping: %w", err)
	}

	res := make([]abstract.TableDescription, 0, len(tables))
	for i, t := range tables {
		var tableColumns []string
		if t.NodeID != nil {
			tableColumns = columns[*t.NodeID]
		}
		parts, err := BuildPartsForTable(ctx, tx, txID, t, partsMapping[i], cfg.GetDesiredPartSizeBytes(), tableColumns)
		if err != nil {
			return nil, xerrors.Errorf("unable to build parts for table '%v': %w", t.OriginalYPath(), err)
		}
		res = append(res, parts...)
	}
	return res, nil
}

// BuildPartsForTable splits a single table using YT SDK partition cookies.
// The table is locked with a snapshot lock (filling in NodeID) when it has not
// been locked yet.
func BuildPartsForTable(
	ctx context.Context,
	tx yt.Tx,
	txID yt.TxID,
	t *cypressmeta.YtNodeMeta,
	shardCount int,
	desiredPartSizeBytes int64,
	columns []string,
) ([]abstract.TableDescription, error) {
	if tx == nil {
		return nil, xerrors.New("yt transaction is required to partition table")
	}
	if shardCount <= 0 {
		return nil, xerrors.Errorf("invalid shard count: %d", shardCount)
	}
	if desiredPartSizeBytes <= 0 {
		desiredPartSizeBytes = 1
	}
	if t.NodeID == nil {
		lock, err := tx.LockNode(ctx, t.OriginalYPath(), yt.LockSnapshot, nil)
		if err != nil {
			return nil, xerrors.Errorf("unable to lock table '%v': %w", t.OriginalYPath(), err)
		}
		t.NodeID = &lock.NodeID
	}

	path := t.NodeID.YPath().Rich()
	if columns != nil {
		path.SetColumns(columns)
	}
	enableCookies := true
	partitionMode := yt.PartitionModeOrdered
	options := &yt.PartitionTablesOptions{
		DataWeightPerPartition: desiredPartSizeBytes,
		MaxPartitionCount:      &shardCount,
		PartitionMode:          &partitionMode,
		EnableCookies:          &enableCookies,
		TransactionOptions:     &yt.TransactionOptions{TransactionID: txID},
	}

	partitions, err := tx.PartitionTables(ctx, []ypath.YPath{path}, options)
	if err != nil {
		return nil, xerrors.Errorf("unable to partition table %s: %w", t.Name, err)
	}

	res := make([]abstract.TableDescription, 0, len(partitions.Partitions))
	for _, partition := range partitions.Partitions {
		part, err := NewPartFromPartition(t.Name, *t.NodeID, partition, txID)
		if err != nil {
			return nil, xerrors.Errorf("unable to build table part from sdk partition: %w", err)
		}
		tablePart, err := part.ToTablePart()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert table part: %w", err)
		}
		res = append(res, *tablePart)
	}
	return res, nil
}

type tableWeightPair struct {
	TableIndex  int
	TableWeight int64
}

// ComputePartsMapping distributes grpcShardLimit slots across tables, giving
// bigger tables proportionally more parts. Every table gets at least 1 part.
// Bails out if more than grpcShardLimit tables are passed.
//
// The rules are preserved verbatim from the legacy uniformParts method for
// bit-compatible sharding behaviour.
func ComputePartsMapping(tables cypressmeta.YtNodes, cfg provider_yt.YtSourceModel, lgr log.Logger) (map[int]int, error) {
	if err := CheckTableCountLimit(tables); err != nil {
		return nil, err
	}
	if cfg.GetDesiredPartSizeBytes() == 0 {
		return nil, xerrors.New("invalid YT provider config: DesiredPartSizeBytes = 0")
	}
	restParts := grpcShardLimit
	tablesWeightArr := make([]tableWeightPair, 0, len(tables))
	var totalWeight int64

	for i, w := range tables {
		totalWeight += w.DataWeight
		tablesWeightArr = append(tablesWeightArr, tableWeightPair{TableIndex: i, TableWeight: w.DataWeight})
	}

	// Compare explicitly: int(a.TableWeight - b.TableWeight) can overflow for
	// large int64 weights and invert the sort order.
	xslices.SortFunc(tablesWeightArr, func(a, b tableWeightPair) int {
		switch {
		case a.TableWeight < b.TableWeight:
			return -1
		case a.TableWeight > b.TableWeight:
			return 1
		default:
			return 0
		}
	})

	res := make(map[int]int)

	if totalWeight < 0 {
		return nil, tablesWeightOverflowErr
	} else if totalWeight == 0 {
		for i := range tables {
			res[i] = 1
		}
	}

	for _, pair := range tablesWeightArr {
		var shards int
		var logReason string
		if pair.TableWeight < cfg.GetDesiredPartSizeBytes() {
			shards = 1
			logReason = "being less than desired part size"
		} else {
			rawShards := float64(restParts) * (float64(pair.TableWeight) / float64(totalWeight))
			if rawShards == 0 {
				shards = 1
				logReason = "being proportionally too small"
			} else if (float64(tables[pair.TableIndex].DataWeight) / rawShards) < float64(cfg.GetDesiredPartSizeBytes()) {
				shards = int(math.Floor(float64(tables[pair.TableIndex].DataWeight) / float64(cfg.GetDesiredPartSizeBytes())))
				logReason = "using desired part size"
			} else {
				shards = int(rawShards)
				logReason = "keeping proportional parts distribution"
			}
		}
		if shards == 0 {
			shards = 1
		}
		restParts -= shards
		totalWeight -= pair.TableWeight
		res[pair.TableIndex] = shards
		if lgr != nil {
			lgr.Infof("Table %s split into %d parts due to %s", tables[pair.TableIndex].OriginalPath(), shards, logReason)
		}
	}
	return res, nil
}
