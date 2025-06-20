//go:build !disable_yt_provider

package dataobjects

import (
	"context"
	"math"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/filter"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/slices"
)

const grpcShardLimit = 1024

var tablesWeightOverflowErr = xerrors.NewSentinel("total tables weight overflow")

type YTDataObjects struct {
	idx          int
	err          error
	tbls         tablemeta.YtTables
	tx           yt.Tx
	txID         yt.TxID
	parts        map[string][]partKey
	currentParts []partKey
	cfg          *yt2.YtSource
	lgr          log.Logger
	filter       base.DataObjectFilter
}

func (objs *YTDataObjects) Next() bool {
	if objs.parts != nil {
		return objs.nextPresharded()
	}
	return objs.nextSharding()
}

func (objs *YTDataObjects) loadTableList() (tablemeta.YtTables, error) {
	paths := objs.cfg.Paths
	if listable, ok := objs.filter.(filter.ListableFilter); ok {
		tables, err := listable.ListTables()
		if err != nil {
			return nil, xerrors.Errorf("unable to list table filter: %w", err)
		}
		var resPaths []string
	TABLES:
		for _, table := range tables {
			for _, path := range objs.cfg.Paths {
				if strings.HasPrefix(table.Name, path) {
					resPaths = append(resPaths, table.Name)
					continue TABLES
				}
			}
			return nil, xerrors.Errorf("unable to find `%s` in source", table.Name)
		}
		paths = resPaths
	}
	tbls, err := tablemeta.ListTables(context.Background(), objs.tx, objs.cfg.Cluster, paths, objs.lgr)
	if err != nil {
		return nil, xerrors.Errorf("error listing tables: %w", err)
	}
	return tbls, nil
}

func (objs *YTDataObjects) nextSharding() bool {
	if objs.tbls == nil {
		objs.tbls, objs.err = objs.loadTableList()
		if objs.err != nil || len(objs.tbls) == 0 {
			return false
		}
		objs.idx = 0
	} else {
		objs.idx++
	}
	if objs.idx >= len(objs.tbls) {
		return false
	}
	tbl := objs.tbls[objs.idx]
	lock, err := objs.tx.LockNode(context.Background(), tbl.OriginalYPath(), yt.LockSnapshot, nil)
	if err != nil {
		objs.err = err
		return false
	}
	tbl.NodeID = &lock.NodeID
	return true
}

func (objs *YTDataObjects) nextPresharded() bool {
	if len(objs.parts) == 0 {
		return false
	}
	var key string
	for k, parts := range objs.parts {
		objs.currentParts = parts
		key = k
		break
	}
	delete(objs.parts, key)
	return true
}

func (objs *YTDataObjects) Err() error {
	return objs.err
}

func (objs *YTDataObjects) Close() {
	if objs.tbls != nil {
		objs.idx = len(objs.tbls) + 1
		return
	}
	objs.currentParts = nil
	objs.parts = nil
}

func (objs *YTDataObjects) Object() (base.DataObject, error) {
	if objs.currentParts != nil {
		return newPreshardedDataObject(objs.txID, objs.currentParts), nil
	}
	if l := len(objs.tbls); objs.idx >= l {
		return nil, xerrors.Errorf("iter index out of range: %d of %d", objs.idx, l)
	}
	return newShardingDataObject(objs.tbls[objs.idx], objs.txID, 1), nil
}

func (objs *YTDataObjects) ToOldTableMap() (abstract.TableMap, error) {
	return nil, xerrors.Errorf("legacy TableMap is not supported")
}

type tableWeightPair struct {
	TableIndex  int
	TableWeight int64
}

// uniformParts uniforms parts in the way, where bigger tables have more parts than little one. Every table gets at least
// 1 part. If there are more than 1024 tables, method will return error.
func (objs *YTDataObjects) uniformParts() (map[int]int, error) {
	if len(objs.tbls) > grpcShardLimit {
		return nil, xerrors.Errorf("%v tables. Can not be more than 1024 tables", len(objs.tbls))
	}
	if objs.cfg.DesiredPartSizeBytes == 0 {
		return nil, xerrors.New("invalid YT provider config: DesiredPartSizeBytes = 0")
	}
	restParts := grpcShardLimit
	tablesWeightArr := make([]tableWeightPair, 0, len(objs.tbls))
	var totalWeight int64

	for i, w := range objs.tbls {
		totalWeight += w.DataWeight
		tablesWeightArr = append(tablesWeightArr, tableWeightPair{TableIndex: i, TableWeight: w.DataWeight})
	}

	slices.SortFunc(tablesWeightArr, func(a, b tableWeightPair) int { return int(a.TableWeight - b.TableWeight) })

	res := make(map[int]int)

	if totalWeight < 0 {
		return nil, tablesWeightOverflowErr
	} else if totalWeight == 0 {
		for i := range objs.tbls {
			res[i] = 1
		}
	}

	for _, pair := range tablesWeightArr {
		var shards int
		var logReason string
		if pair.TableWeight < objs.cfg.DesiredPartSizeBytes {
			shards = 1
			logReason = "being less than desired part size"
		} else {
			rawShards := float64(restParts) * (float64(pair.TableWeight) / float64(totalWeight))
			if rawShards == 0 {
				shards = 1
				logReason = "being proportionally too small"
			} else if (float64(objs.tbls[pair.TableIndex].DataWeight) / rawShards) < float64(objs.cfg.DesiredPartSizeBytes) {
				shards = int(math.Floor(float64(objs.tbls[pair.TableIndex].DataWeight) / float64(objs.cfg.DesiredPartSizeBytes)))
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
		objs.lgr.Infof("Table %s split into %d parts due to %s", objs.tbls[pair.TableIndex].OriginalPath(), shards, logReason)

	}
	return res, nil
}

func (objs *YTDataObjects) ToTableParts() ([]abstract.TableDescription, error) {
	if objs.tbls == nil {
		objs.tbls, objs.err = objs.loadTableList()
		if objs.err != nil {
			return nil, xerrors.Errorf("unable to init table list: %w", objs.err)
		}
	}

	partsMapping, err := objs.uniformParts()
	if err != nil {
		return nil, err
	}

	tableDescriptions := []abstract.TableDescription{}
	for i, t := range objs.tbls {
		lock, err := objs.tx.LockNode(context.Background(), t.OriginalYPath(), yt.LockSnapshot, nil)
		if err != nil {
			return nil, xerrors.Errorf("unable to lock table '%v': %w", t.OriginalYPath(), objs.err)
		}
		t.NodeID = &lock.NodeID

		o := newShardingDataObject(t, objs.txID, partsMapping[i])
		for o.Next() {
			tableDescription, err := o.part().ToTablePart()
			if err != nil {
				return nil, xerrors.Errorf("error serializing table part to table description: %w", err)
			}
			tableDescriptions = append(tableDescriptions, *tableDescription)
		}
	}

	return tableDescriptions, nil
}

func (objs *YTDataObjects) ParsePartKey(data string) (*abstract.TableID, error) {
	partKey, err := ParsePartKey(data)
	if err != nil {
		return nil, err
	}
	return abstract.NewTableID("", partKey.Table), nil
}

func NewDataObjects(cfg *yt2.YtSource, tx yt.Tx, lgr log.Logger, filter base.DataObjectFilter) *YTDataObjects {
	var txID yt.TxID
	if tx != nil {
		txID = tx.ID()
	}
	return &YTDataObjects{
		idx:          -1,
		err:          nil,
		tbls:         nil,
		tx:           tx,
		txID:         txID,
		parts:        nil,
		currentParts: nil,
		cfg:          cfg,
		lgr:          lgr,
		filter:       filter,
	}
}
