package clickhouse

import (
	"context"
	"fmt"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type ShardStorage struct {
	shards map[string]*Storage
	logger log.Logger
}

func (s *ShardStorage) TableParts(ctx context.Context, table abstract.TableID) ([]TablePart, error) {
	var res []TablePart
	for name, shard := range s.shards {
		parts, err := shard.TableParts(ctx, table)
		if err != nil {
			return nil, xerrors.Errorf("unable to load parts from shard %v: %w", name, err)
		}
		for _, part := range parts {
			res = append(res, TablePart{
				Name:       part.Name,
				Rows:       part.Rows,
				Bytes:      part.Bytes,
				Shard:      name,
				Table:      table,
				ShardCount: len(s.shards),
			})
		}
	}
	return res, nil
}

func (s *ShardStorage) Version() semver.Version {
	return s.defaultShard().Version()
}

func (s *ShardStorage) LoadTablesDDL(tables []abstract.TableID) ([]schema.TableDDL, error) {
	return s.defaultShard().LoadTablesDDL(tables)
}

func (s *ShardStorage) BuildTableQuery(table abstract.TableDescription) (*abstract.TableSchema, string, string, error) {
	return s.defaultShard().BuildTableQuery(table)
}

func (s *ShardStorage) GetRowsCount(tableID abstract.TableID) (uint64, error) {
	distr, err := s.defaultShard().isDistributed(tableID)
	if err != nil {
		return 0, xerrors.Errorf("unable to check table engine %v: %w", tableID.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().GetRowsCount(tableID)
	}
	var errs util.Errors
	total := uint64(0)
	for name, shard := range s.shards {
		sc, err := shard.GetRowsCount(tableID)
		if err != nil {
			errs = append(errs, xerrors.Errorf("unable to row count for shard: %v: %v", name, err))
		}
		total += sc
	}
	if len(errs) > 0 {
		return 0, xerrors.Errorf("unable to row count sharded storage: %w", errs)
	}
	return total, nil
}

func (s *ShardStorage) Close() {
	for _, shard := range s.shards {
		shard.Close()
	}
}

func (s *ShardStorage) Ping() error {
	var errs util.Errors
	for _, shard := range s.shards {
		if err := shard.Ping(); err != nil {
			errs = append(errs, xerrors.Errorf("unable to ping shard: %v: %v", s, err))
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to ping: %w", errs)
	}
	return nil
}

func (s *ShardStorage) TableList(f abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.defaultShard().TableList(f)
}

func (s *ShardStorage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	distr, err := s.defaultShard().isDistributed(table)
	if err != nil {
		return 0, xerrors.Errorf("unable to check table engine %v: %w", table.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().TableSizeInBytes(table)
	}
	var errs util.Errors
	total := uint64(0)
	for name, shard := range s.shards {
		sc, err := shard.TableSizeInBytes(table)
		if err != nil {
			errs = append(errs, xerrors.Errorf("unable to get table size for shard: %v: %v", name, err))
		}
		total += sc
	}
	if len(errs) > 0 {
		return 0, xerrors.Errorf("unable to get table size for sharded ch: %w", errs)
	}
	return total, nil
}

func (s *ShardStorage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.defaultShard().TableSchema(ctx, table)
}

func (s *ShardStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	distr, err := s.defaultShard().isDistributed(table.ID())
	if err != nil {
		return xerrors.Errorf("unable to check table engine %v: %w", table.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().LoadTable(ctx, table, pusher)
	}

	partID := table.PartID()

	subCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, len(s.shards))
	for name, shard := range s.shards {
		go func(name string, shard *Storage) {
			err := shard.LoadTable(subCtx, table, func(input []abstract.ChangeItem) error {
				onlyRowEvents := true
				for i, r := range input {
					if !r.IsRowEvent() {
						onlyRowEvents = false
					}
					input[i].PartID = partID
				}
				if !onlyRowEvents {
					return nil
				}
				return pusher(input)
			})
			if err != nil {
				errCh <- xerrors.Errorf("unable to load table for shard %v: %w", name, err)
			} else {
				errCh <- nil
			}
		}(name, shard)
	}
	for range s.shards {
		err := <-errCh
		if err != nil {
			cancel()
			return xerrors.Errorf("unable to load sharded table: %w", err)
		}
	}
	cancel()

	return nil
}

func (s *ShardStorage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	distr, err := s.defaultShard().isDistributed(table.ID())
	if err != nil {
		return xerrors.Errorf("unable to check table engine %v: %w", table.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().LoadTopBottomSample(table, pusher)
	}
	var errs util.Errors
	for name, shard := range s.shards {
		if err := shard.LoadTopBottomSample(table, pusher); err != nil {
			errs = append(errs, xerrors.Errorf("unable to load top-bottom sample for shard: %v: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to load top-bottom sample for sharded clickhouse: %w", errs)
	}
	return nil
}

func (s *ShardStorage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	distr, err := s.defaultShard().isDistributed(table.ID())
	if err != nil {
		return xerrors.Errorf("unable to check table engine %v: %w", table.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().LoadRandomSample(table, pusher)
	}
	var errs util.Errors
	for name, shard := range s.shards {
		if err := shard.LoadRandomSample(table, pusher); err != nil {
			errs = append(errs, xerrors.Errorf("unable to load random sample for shard: %v: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to load random sample for sharded clickhouse: %w", errs)
	}
	return nil
}

func (s *ShardStorage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	distr, err := s.defaultShard().isDistributed(table.ID())
	if err != nil {
		return xerrors.Errorf("unable to check table engine %v: %w", table.Fqtn(), err)
	}
	if distr {
		return s.defaultShard().LoadSampleBySet(table, keySet, pusher)
	}
	var errs util.Errors
	for name, shard := range s.shards {
		if err := shard.LoadSampleBySet(table, keySet, pusher); err != nil {
			errs = append(errs, xerrors.Errorf("unable to load keyset for shard: %v: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to keyset for sharded clickhouse: %w", errs)
	}
	return nil
}

func (s *ShardStorage) TableAccessible(table abstract.TableDescription) bool {
	return s.defaultShard().TableAccessible(table)
}

func (s *ShardStorage) defaultShard() *Storage {
	for name := range s.shards {
		return s.shards[name]
	}
	return nil
}

func (s *ShardStorage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	var total uint64

	for _, shard := range s.shards {
		rowsCount, err := shard.ExactTableRowsCount(table)
		if err != nil {
			return 0, xerrors.Errorf("failed to get exact rows count for table %s: %w", table, err)
		}
		total += rowsCount
	}

	return total, nil
}

func (s *ShardStorage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func (s *ShardStorage) TableExists(table abstract.TableID) (bool, error) {
	for _, shard := range s.shards {
		exists, err := shard.TableExists(table)
		if err != nil {
			return false, xerrors.Errorf("unable to check table existence: %v", err)
		}

		if exists {
			return true, nil
		}
	}

	return false, nil
}

func (s *ShardStorage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	var res []abstract.IncrementalState
	for _, currTable := range incremental {
		var maxVal string

		for _, storage := range s.shards {
			val, err := getMaxCursorFieldValue(ctx, storage.db, currTable)
			if err != nil {
				return nil, err
			}

			stringVal, ok := val.(string)
			if !ok {
				return nil, xerrors.New("shard max value type is not string")
			}

			if stringVal > maxVal || maxVal == "" {
				maxVal = stringVal
			}
		}

		res = append(res, abstract.IncrementalState{
			Name:    currTable.Name,
			Schema:  currTable.Namespace,
			Payload: abstract.WhereStatement(fmt.Sprintf(`"%s" > parseDateTime64BestEffort('%s', 9)`, currTable.CursorField, maxVal)),
		})
	}
	return res, nil
}

func (s *ShardStorage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incremental []abstract.IncrementalTable) []abstract.TableDescription {
	return setInitialState(tables, incremental)
}

func NewShardedStorage(shards map[string]*Storage) ClickhouseStorage {
	return &ShardStorage{shards: shards, logger: logger.Log}
}

func NewShardedFromUrls(config *model.ChStorageParams, transfer *dp_model.Transfer, opts ...StorageOpt) (ClickhouseStorage, error) {
	shards := map[string]*Storage{}
	allShardsAreSingleHost := slices.IndexFunc(maps.Values(config.ConnectionParams.Shards),
		func(hosts []*clickhouse.Host) bool { return len(hosts) > 1 }) == -1
	for name := range config.ConnectionParams.Shards {
		db, err := makeShardConnection(config, name)
		if err != nil {
			return nil, xerrors.Errorf("unable to init connection for shard %v: %w", name, err)
		}
		opts = append(opts, WithShardName(name))
		version, err := backoff.RetryWithData(func() (string, error) {
			var version string

			if err := db.QueryRow("select version();").Scan(&version); err != nil {
				return "", xerrors.Errorf("unable to select clickhouse version: %w", err)
			}
			return version, nil
		}, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, xerrors.Errorf("unable to extract version: %w", err)
		}
		parsedVersion, err := parseSemver(version)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse semver: %w", err)
		}
		st := WithOpts(&Storage{
			db:        db,
			database:  config.ConnectionParams.Database,
			cluster:   config.ChClusterName,
			bufSize:   config.BufferSize,
			logger:    logger.Log,
			onceClose: sync.Once{},
			IsHomo:    false,
			metrics:   stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
			version:   *parsedVersion,

			tableFilter:           nil,
			allowSingleHostTables: allShardsAreSingleHost,
		}, opts...)
		shards[name] = st
	}
	return NewShardedStorage(shards), nil
}
