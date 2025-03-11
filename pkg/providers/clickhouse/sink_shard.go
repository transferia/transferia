package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/clickhouse/errors"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/topology"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type lazySinkShard struct {
	Name     string
	Config   model.ChSinkShardParams
	logger   log.Logger
	registry metrics.Registry
	sink     *sinkShard
	topology *topology.Topology
}

func newLazySinkShard(shardName string, config model.ChSinkShardParams, topology *topology.Topology, logger log.Logger, registry metrics.Registry) *lazySinkShard {
	return &lazySinkShard{
		Name:     shardName,
		Config:   config,
		logger:   logger,
		registry: registry,
		sink:     nil,
		topology: topology,
	}
}

func (ls *lazySinkShard) Sink() (*sinkShard, error) {
	if ls.sink == nil {
		subRegistry := ls.registry.WithTags(map[string]string{"shard": ls.Name})
		result, err := newSinkShard(
			ls.Name,
			ls.Config,
			ls.topology,
			log.With(ls.logger, log.String("shard", ls.Name)),
			stats.NewChStats(subRegistry),
			stats.NewSinkerStats(subRegistry),
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to create a sink for shard %q: %w", ls.Name, err)
		}
		ls.sink = result
	}

	return ls.sink, nil
}

func (ls *lazySinkShard) SinkIfInitialized() *sinkShard {
	return ls.sink
}

type sinkShard struct {
	shardName      string
	cluster        *sinkCluster
	config         model.ChSinkShardParams
	logger         log.Logger
	metrics        *stats.SinkerStats
	chStats        *stats.ChStats
	altNames       map[string]string
	closeReasonErr error
	topology       *topology.Topology
}

func (s *sinkShard) Close() error {
	if err := s.cluster.Close(); err != nil {
		if s.closeReasonErr != nil {
			s.logger.Warn("ClickHouse sinkShard cluster close failed", log.Error(err))
		} else {
			s.closeReasonErr = err
		}
	}
	if s.closeReasonErr != nil {
		return xerrors.Errorf("failed while closing ClickHouse sinkShard: %w", s.closeReasonErr)
	}
	s.logger.Debug("ClickHouse sinkShard closed without errors")
	return nil
}

func (s *sinkShard) reset() {
	if err := backoff.RetryNotify(
		s.cluster.Reset,
		backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second*15), 3),
		util.BackoffLogger(s.logger, "sinkShard reset"),
	); err != nil {
		s.logger.Error("sinkShard reset failed", log.Error(err))
	}

	s.closeReasonErr = xerrors.New("close sinkShard for resetting")
	if err := s.Close(); err != nil {
		s.logger.Error("Failed to close sinkShard", log.Error(err))
	}
	err := backoff.Retry(func() error {
		cl, err := newSinkCluster(s.config, s.logger, s.chStats, s.topology)
		s.cluster = cl
		return err
	}, backoff.NewExponentialBackOff())
	if err != nil {
		s.logger.Error("unable to reset sinkShard", log.Error(err))
		s.closeReasonErr = err
		if err := s.Close(); err != nil {
			s.logger.Error("Failed to close sinkShard", log.Error(err))
		}
	}
}

func (s *sinkShard) Push(input []abstract.ChangeItem) error {
	var err error
	for i := 0; i < s.config.RetryCount(); i++ {
		for i := 0; i < s.config.RetryCount(); i++ {
			err = s.pushBatch(input)
			if err == nil || err == sql.ErrTxDone {
				return nil
			}

			if abstract.IsFatal(err) {
				return xerrors.Errorf("ClickHouse Push failed (got fatal error): %w", err)
			}

			var ddlTaskErr errors.DDLTaskError
			// No reason to fill DDL task queue with retries on half-dead cluster
			if xerrors.As(err, &ddlTaskErr) {
				return xerrors.Errorf("ddl task error: %w", err)
			}

			exception := new(clickhouse.Exception)
			if xerrors.As(err, &exception) {
				if !errors.RetryableCode[exception.Code] {
					s.logger.Warn("Non-retriable ClickHouse error", log.Any("attempt", i), log.Any("stack", exception.StackTrace), log.Error(err))
					return err
				}
				s.logger.Warn("ClickHouse Retry", log.Any("attempt", i), log.Any("stack", exception.StackTrace), log.Error(err))
				time.Sleep(5 + time.Second*time.Duration(i))
				continue
			}

			if err == driver.ErrBadConn {
				s.logger.Warn("ClickHouse Retry", log.Any("attempt", i), log.Error(err))
				s.reset()
				time.Sleep(5 + time.Second*time.Duration(i))
				continue
			}
			break
		}
		s.reset()
		s.logger.Warn("Retrying push error", log.Error(err), log.Int("attempt", i))
	}
	return xerrors.Errorf("ClickHouse Push failed after %d attempts; last error: %w", s.config.RetryCount(), err)
}

type opStats struct {
	upserted int
	deleted  int
}

func (s *sinkShard) tableName(row abstract.ChangeItem) string {
	var targetTable string
	if s.config.UseSchemaInTableName() && row.Schema != "" {
		targetTable = normalizeTableName(row.Schema + "_" + row.Table)
	} else {
		targetTable = normalizeTableName(row.Table)
	}

	if s.altNames[targetTable] != "" {
		targetTable = s.altNames[targetTable]
	}
	return targetTable
}

func (s *sinkShard) pushBatch(input []abstract.ChangeItem) error {
	var ops int
	start := time.Now()

	rowsByTables := make(map[string][]abstract.ChangeItem)
	statByTables := make(map[string]*opStats)

	for _, row := range input {
		targetTable := s.tableName(row)

		switch row.Kind {
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			if rowsByTables[targetTable] == nil {
				rowsByTables[targetTable] = make([]abstract.ChangeItem, 0)
				statByTables[targetTable] = new(opStats)
			}
			rowsByTables[targetTable] = append(rowsByTables[targetTable], row)
			if row.Kind == abstract.DeleteKind {
				statByTables[targetTable].deleted++
			} else {
				statByTables[targetTable].upserted++
			}
		case abstract.DropTableKind:
			if err := s.cluster.DropTable(targetTable); err != nil {
				return xerrors.Errorf("unable to drop: %v:%w", targetTable, err)
			}
		case abstract.TruncateTableKind:
			if err := s.cluster.TruncateTable(targetTable); err != nil {
				return xerrors.Errorf("unable to truncate: %v:%w", targetTable, err)
			}
		case abstract.ChCreateTableKind, abstract.ChCreateTableDistributedKind:
			if len(row.ColumnValues) < 2 {
				return abstract.NewFatalError(xerrors.Errorf("to small event packet: %v", len(row.ColumnValues)))
			}
			ddl, ok := row.ColumnValues[0].(string)
			if !ok {
				return abstract.NewFatalError(xerrors.Errorf("unexpected event format: %T", row.ColumnValues[0]))
			}
			err := s.cluster.execDDL(func(distributed bool) error {
				if err := s.cluster.bestSinkServer().ExecDDL(context.Background(), ddl); err != nil {
					return xerrors.Errorf("cannot drop table (distributed=%v): %w", distributed, err)
				}
				return nil
			})
			if err != nil {
				return xerrors.Errorf("unable to create table from DDL: %w", err)
			}
			s.logger.Infof("ddl completed: %v", ddl)
		case abstract.InitShardedTableLoad, abstract.InitTableLoad, abstract.DoneTableLoad, abstract.DoneShardedTableLoad:
			// pass
		case abstract.ClickhouseDDLBuilderKind:
			row.Table = targetTable
			if err := s.execMetrikaDDL(row); err != nil {
				return xerrors.Errorf("error creating target table for metrika transfer: %w", err)
			}
		default:
			s.logger.Infof("ClickHouse does not support %v", row.Kind)
			continue
		}
	}

	for table, rows := range rowsByTables {
		before := time.Now()
		if err := s.cluster.Insert(&TableSpec{
			Name:   table,
			Schema: rows[0].TableSchema,
		}, rows); err != nil {
			s.metrics.Table(table, "error", 1)
			s.logger.Error("Unable to insert", log.Error(err))
			return err
		}
		stat := statByTables[table]
		s.metrics.Table(table, "rows", stat.upserted)
		s.metrics.Table(table, "rows_deleted", stat.deleted)
		ops += len(rows)

		s.logger.Debug(
			"Committed",
			log.Any("table", table),
			log.Any("elapsed", time.Since(before)),
			log.Any("ops", len(rows)),
		)
	}

	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return nil
}

func (s *sinkShard) execMetrikaDDL(row abstract.ChangeItem) error {
	return s.cluster.execDDL(func(distributed bool) error {
		ddl, err := s.buildMetrikaDDL(row, distributed)
		if err != nil {
			return xerrors.Errorf("error building metrika DDL: %w", err)
		}
		return s.cluster.bestSinkServer().ExecDDL(context.Background(), ddl)
	})
}

func (s *sinkShard) buildMetrikaDDL(row abstract.ChangeItem, distributed bool) (string, error) {
	type clickHouseBuilder interface {
		BuildClickHouseDDL(db, table, cluster string, distributed bool) (string, error)
	}
	builder, ok := row.ColumnValues[0].(clickHouseBuilder)
	if !ok {
		return "", xerrors.New("unable to get ClickHouse ddl Builder")
	}
	return builder.BuildClickHouseDDL(s.config.Database(), row.Table, s.cluster.topology.ClusterName(), distributed)
}

func MakeAltNames(config model.ChSinkShardParams) map[string]string {
	var fromTables []string
	for fromTable := range config.Tables() {
		fromTables = append(fromTables, fromTable)
	}

	altNames := map[string]string{}
	for _, fromTable := range fromTables {
		toName := config.Tables()[fromTable]

		// default name
		altNames[fromTable] = toName

		// normalized name
		normalizedFromTable := normalizeTableName(fromTable)
		if fromTable != normalizedFromTable {
			altNames[normalizedFromTable] = toName
		}
	}
	return altNames
}

func newSinkShard(shardName string, config model.ChSinkShardParams, topology *topology.Topology, logger log.Logger, chStats *stats.ChStats, sinkStats *stats.SinkerStats) (*sinkShard, error) {
	cl, err := newSinkCluster(config, logger, chStats, topology)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a sink for a concrete ClickHouse cluster: %w", err)
	}

	s := &sinkShard{
		shardName:      shardName,
		cluster:        cl,
		config:         config,
		logger:         logger,
		metrics:        sinkStats,
		chStats:        chStats,
		altNames:       MakeAltNames(config),
		closeReasonErr: nil,
		topology:       topology,
	}

	return s, nil
}
