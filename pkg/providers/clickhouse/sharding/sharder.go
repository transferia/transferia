package sharding

import (
	"encoding/json"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func hash(val string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(val))
	return int(h.Sum32())
}

func ColumnShardingKeyGen(col string) func(row abstract.ChangeItem) string {
	return func(row abstract.ChangeItem) string {
		idx := row.ColumnNameIndex(col)
		if idx == -1 {
			return "default"
		}
		val := row.ColumnValues[idx]
		switch v := val.(type) {
		case string:
			return v
		default:
			b, _ := json.Marshal(v)
			return string(b)
		}
	}
}

func ShardsFromSinkParams(shards map[string][]*clickhouse.Host) ShardMap[[]*clickhouse.Host] {
	names := maps.Keys(shards)
	slices.Sort(names)

	res := make(ShardMap[[]*clickhouse.Host])
	for idx, name := range names {
		res[ShardID(idx)] = shards[name]
	}
	return res
}

func ConstSharder() Sharder {
	return func(row abstract.ChangeItem) ShardID { return ShardID(0) }
}

func RoundRobinSharder(shardCnt int) Sharder {
	var counter uint
	return func(row abstract.ChangeItem) ShardID {
		counter++
		return ShardID(counter % uint(shardCnt))
	}
}

func KeyGenHashHandler(keygen func(row abstract.ChangeItem) string, shardCnt int) Sharder {
	return func(row abstract.ChangeItem) ShardID {
		key := keygen(row)
		return ShardID(hash(key) % shardCnt)
	}
}

func KeyGenUserMappingHandler(keygen func(row abstract.ChangeItem) string, mapping map[string]int, shardsCnt int) Sharder {
	return func(row abstract.ChangeItem) ShardID {
		key := keygen(row)
		shardIdx, ok := mapping[key]
		if ok {
			return ShardID(shardIdx)
		}
		if strings.HasPrefix(key, "rt3.") {
			dc := key[4:7]
			rawPar := strings.SplitAfter(key, "@")
			part, err := strconv.Atoi(rawPar[len(rawPar)-1])
			if err != nil {
				shardIdx = 0
			} else {
				dcIdx := mapDc(dc)
				shardIdx = (5*part + dcIdx) % shardsCnt
			}
		}
		mapping[key] = shardIdx
		return ShardID(shardIdx)
	}
}

func GetShardIndexUserMapping(cfg model.ChSinkParams) map[string]int {
	shardNamesSorted := make([]string, 0)
	for sName := range cfg.Shards() {
		shardNamesSorted = append(shardNamesSorted, sName)
	}
	sort.Strings(shardNamesSorted)

	shardIndexUserMapping := map[string]int{}
	if len(cfg.ColumnToShardName()) > 0 {
		shardNameToIndex := make(map[string]int)
		for i, shardName := range shardNamesSorted {
			shardNameToIndex[shardName] = i
		}
		for columnValue, shardName := range cfg.ColumnToShardName() {
			shardIndexUserMapping[columnValue] = shardNameToIndex[shardName]
		}
	}
	return shardIndexUserMapping
}

func CHSharder(cfg model.ChSinkParams, transferID string) Sharder {
	var keyGen func(row abstract.ChangeItem) string
	if cfg.ShardByTransferID() {
		keyGen = func(row abstract.ChangeItem) string { return transferID }
	} else if cfg.ShardCol() != "" {
		keyGen = ColumnShardingKeyGen(cfg.ShardCol())
	}
	if keyGen != nil {
		if len(cfg.ColumnToShardName()) > 0 {
			return KeyGenUserMappingHandler(keyGen, GetShardIndexUserMapping(cfg), len(cfg.Shards()))
		} else {
			return KeyGenHashHandler(keyGen, len(cfg.Shards()))
		}
	} else if cfg.ShardByRoundRobin() {
		return RoundRobinSharder(len(cfg.Shards()))
	} else {
		return ConstSharder()
	}
}

func mapDc(s string) int {
	switch s {
	case "iva":
		return 1
	case "man":
		return 2
	case "vla":
		return 3
	case "sas":
		return 4
	case "myt":
		return 5
	}
	return 1
}
