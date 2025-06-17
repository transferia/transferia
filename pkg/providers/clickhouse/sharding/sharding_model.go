//go:build !disable_clickhouse_provider

package sharding

import (
	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/exp/maps"
)

type ShardID int

type Shards[T any] interface {
	Shard(id ShardID) T
	Shards() []ShardID
}

type Sharder func(row abstract.ChangeItem) ShardID

type ShardMap[T any] map[ShardID]T

func (s ShardMap[T]) Shard(id ShardID) T {
	return s[id]
}

func (s ShardMap[T]) Shards() []ShardID {
	return maps.Keys(s)
}
