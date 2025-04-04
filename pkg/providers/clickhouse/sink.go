package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	topology2 "github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var ClickHouseSinkClosedErr = xerrors.New("ClickHouse sink has already been closed")

type sink struct {
	// mu ensures the rotator is not concurrent with other operations on sink
	mu                    sync.Mutex
	closed                bool
	onceClose             sync.Once
	shardIndexUserMapping map[string]int
	config                model.ChSinkParams
	logger                log.Logger
	metrics               metrics.Registry
	transferID            string
	runtime               abstract.Runtime
	sharder               sharding.Sharder
	shardMap              sharding.ShardMap[*lazySinkShard]
}

func (s *sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	errors := util.NewErrs()

	for i, ls := range s.shardMap {
		sink := ls.SinkIfInitialized()
		if sink != nil {
			if err := sink.Close(); err != nil {
				errors = util.AppendErr(errors, xerrors.Errorf("failed to close shard %d: %w", i, err))
			}
		}
	}
	s.closed = true

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (s *sink) Push(input []abstract.ChangeItem) error {
	if s.isClosed() {
		return ClickHouseSinkClosedErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	shardToChangeItems := make(map[sharding.ShardID][]abstract.ChangeItem)
	for _, row := range input {
		idx := s.sharder(row)
		shardToChangeItems[idx] = append(shardToChangeItems[idx], row)
	}
	s.logger.Info(
		"Incoming batch of ChangeItems split by ClickHouse shards successfully",
		log.Int("len", len(input)),
		log.Array("distribution", func() []string {
			result := make([]string, 0)
			for shard, itemsForShard := range shardToChangeItems {
				result = append(result, fmt.Sprintf("%d: %d", shard, len(itemsForShard)))
			}
			return result
		}()),
	)

	var wg sync.WaitGroup
	var errs = make([]error, len(shardToChangeItems))
	i := 0
	for shard, itemsForShard := range shardToChangeItems {
		wg.Add(1)
		go func(shardIdx sharding.ShardID, batch []abstract.ChangeItem, i int) {
			defer wg.Done()
			cluster, err := s.shardMap[shardIdx].Sink()
			if err != nil {
				errs[i] = xerrors.Errorf("failed to get a ClickHouse sink for shard %d: %w", shardIdx, err)
				return
			}
			if err := cluster.Push(batch); err != nil {
				errs[i] = xerrors.Errorf("failed to push %d rows to ClickHouse shard %d: %w", len(batch), shardIdx, err)
			}
		}(shard, itemsForShard, i)
		i++
	}
	wg.Wait()
	err := errors.Join(errs...)
	if err != nil {
		if slices.ContainsFunc(errs, abstract.IsFatal) {
			return abstract.NewFatalError(err)
		}
		return err
	}
	return nil
}

func (s *sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *sink) runRotator() {
	for {
		time.Sleep(time.Minute * 15)
		if s.isClosed() {
			return
		}
		s.logger.Info("Running rotation in ClickHouse sink")
		if err := s.rotate(); err != nil {
			s.logger.Warn("ClickHouse sink rotation failed", log.Error(err))
		}
	}
}

func (s *sink) rotate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.config.Tables() {
		for _, shard := range s.shardMap {
			sink := shard.SinkIfInitialized()
			if sink == nil {
				continue
			}
			if err := sink.cluster.RemoveOldParts(s.config.Rotation().KeepPartCount, v); err != nil {
				return xerrors.Errorf("failed to clean up shard %q: %w", shard.Name, err)
			}
		}
	}
	return nil
}

func newSinkImpl(transfer *dp_model.Transfer, config model.ChSinkParams, logger log.Logger, metrics metrics.Registry, runtime abstract.Runtime) (*sink, error) {
	if err := conn.ResolveShards(config, transfer); err != nil {
		return nil, xerrors.Errorf("Can't resolve shards: %w", err)
	}

	topology, err := topology2.ResolveTopology(config, logger)
	if err != nil {
		return nil, xerrors.Errorf("error resolving cluster topology: %w", err)
	}

	shardNamesSorted := make([]string, 0)
	for sName := range config.Shards() {
		shardNamesSorted = append(shardNamesSorted, sName)
	}
	sort.Strings(shardNamesSorted)

	shardIndexUserMapping := map[string]int{}
	if len(config.ColumnToShardName()) > 0 {
		shardNameToIndex := make(map[string]int)
		for i, shardName := range shardNamesSorted {
			shardNameToIndex[shardName] = i
		}
		for columnValue, shardName := range config.ColumnToShardName() {
			shardIndexUserMapping[columnValue] = shardNameToIndex[shardName]
		}
	}

	shardMap := sharding.ShardMap[*lazySinkShard]{}
	for shardIdx, shardName := range shardNamesSorted {
		shard := *newLazySinkShard(shardName, config.MakeChildShardParams(config.Shards()[shardName]), topology, logger, metrics)
		shardMap[sharding.ShardID(shardIdx)] = &shard
	}

	result := &sink{
		mu:                    sync.Mutex{},
		closed:                false,
		onceClose:             sync.Once{},
		shardIndexUserMapping: shardIndexUserMapping,
		config:                config,
		logger:                logger,
		metrics:               metrics,
		transferID:            transfer.ID,
		runtime:               runtime,
		sharder:               sharding.CHSharder(config, transfer.ID),
		shardMap:              shardMap,
	}

	if result.config.Rotation() != nil {
		go result.runRotator()
	}

	return result, nil
}

func NewSink(transfer *dp_model.Transfer, logger log.Logger, metrics metrics.Registry, runtime abstract.Runtime, middlewaresConfig middlewares.Config) (abstract.Sinker, error) {
	dst, ok := transfer.Dst.(*model.ChDestination)
	if !ok {
		panic("expected ClickHouse destination in ClickHouse sink constructor")
	}

	uncasted, err := newSinkImpl(transfer, dst.ToSinkParams(transfer), logger, metrics, runtime)
	if err != nil {
		return nil, xerrors.Errorf("failed to create pure ClickHouse sink: %w", err)
	}
	var result abstract.Sinker
	result = uncasted

	if !middlewaresConfig.NoData && dst.Interval > 0 {
		result = middlewares.IntervalThrottler(logger, dst.Interval)(uncasted)
	}
	return result, nil
}
