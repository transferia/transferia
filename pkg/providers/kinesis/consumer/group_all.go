//go:build !disable_kinesis_provider

package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

// NewAllGroup returns an intitialized AllGroup for consuming
// all shards on a stream
func NewAllGroup(ksis kinesisiface.KinesisAPI, store Store, streamName string, logger log.Logger) *AllGroup {
	return &AllGroup{
		Store:      store,
		ksis:       ksis,
		streamName: streamName,
		logger:     logger,
		shardMu:    sync.Mutex{},
		shards:     make(map[string]*kinesis.Shard),
	}
}

// AllGroup is used to consume all shards from a single consumer. It
// caches a local list of the shards we are already processing
// and routinely polls the stream looking for new shards to process.
type AllGroup struct {
	Store

	ksis       kinesisiface.KinesisAPI
	streamName string
	logger     log.Logger

	shardMu sync.Mutex
	shards  map[string]*kinesis.Shard
}

// Start is a blocking operation which will loop and attempt to find new
// shards on a regular cadence.
func (g *AllGroup) Start(ctx context.Context, shardc chan *kinesis.Shard) {
	ticker := time.NewTicker(30 * time.Second)
	g.findNewShards(shardc)

	// Note: while ticker is a rather naive approach to this problem,
	// it actually simplies a few things. i.e. If we miss a new shard while
	// AWS is resharding we'll pick it up max 30 seconds later.

	// It might be worth refactoring this flow to allow the consumer to
	// to notify the broker when a shard is closed. However, shards don't
	// necessarily close at the same time, so we could potentially get a
	// thundering heard of notifications from the consumer.

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			g.findNewShards(shardc)
		}
	}
}

// findNewShards pulls the list of shards from the Kinesis API
// and uses a local cache to determine if we are already processing
// a particular shard.
func (g *AllGroup) findNewShards(shardc chan *kinesis.Shard) {
	g.shardMu.Lock()
	defer g.shardMu.Unlock()

	shards, err := listShards(g.ksis, g.streamName)
	if err != nil {
		g.logger.Warn("list shard failed error", log.Error(err))
		return
	}

	for _, shard := range shards {
		if _, ok := g.shards[*shard.ShardId]; ok {
			continue
		}
		g.shards[*shard.ShardId] = shard
		shardc <- shard
	}
}

// listShards pulls a list of shard IDs from the kinesis api
func listShards(ksis kinesisiface.KinesisAPI, streamName string) ([]*kinesis.Shard, error) {
	var ss []*kinesis.Shard
	listShardsInput := &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	for {
		resp, err := ksis.ListShards(listShardsInput)
		if err != nil {
			return nil, xerrors.Errorf("ListShards failed: %w", err)
		}
		ss = append(ss, resp.Shards...)

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken: resp.NextToken,
		}
	}
}
