package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/providers/oracle/logtracker"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	oracle_snapshot "github.com/transferia/transferia/pkg/providers/oracle/snapshot"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ abstract.Storage = (*OracleStorage)(nil)
)

// OracleStorage implements abstract.Storage for Oracle snapshot reads.
type OracleStorage struct {
	sqlxDB             *sqlx.DB
	logger             log.Logger
	registry           core_metrics.Registry
	sourceStats        *stats.SourceStats
	config             provider_oracle.OracleSource
	databaseSchema     *oracle_schema.Database
	tracker            logtracker.LogTracker
	initOnce           sync.Once
	initErr            error
	snapshotShardsNum  int    // number of workers for sharded snapshot; 0 or 1 = no sharding
	shardedSCN         uint64 // SCN shared from main worker; 0 = not set
	rowIDBytesPerShard uint64 // bytes per ROWID range for ShardTable; 0 = use const default
}

func NewOracleStorage(
	logger log.Logger,
	registry core_metrics.Registry,
	cp coordinator.Coordinator,
	config *provider_oracle.OracleSource,
	transferID string,
	snapshotShardsNum int,
) (*OracleStorage, error) {
	sqlxDB, err := oracle_common.CreateConnection(config)
	if err != nil {
		return nil, xerrors.Errorf("Can't create connection: %w", err)
	}

	schemaRepo, err := oracle_schema.NewDatabase(sqlxDB, config, logger)
	if err != nil {
		return nil, xerrors.Errorf("Can't create schema repository: %w", err)
	}

	tracker, err := createLogTracker(sqlxDB, cp, config, transferID)
	if err != nil {
		return nil, xerrors.Errorf("Can't create log tracker: %w", err)
	}

	//nolint:exhaustivestruct
	return &OracleStorage{
		sqlxDB:             sqlxDB,
		logger:             logger,
		registry:           registry,
		sourceStats:        stats.NewSourceStats(registry),
		config:             *config,
		databaseSchema:     schemaRepo,
		tracker:            tracker,
		snapshotShardsNum:  snapshotShardsNum,
		rowIDBytesPerShard: config.RowIDBytesPerShard,
	}, nil
}

func (s *OracleStorage) ensureInit() error {
	s.initOnce.Do(func() {
		if err := s.databaseSchema.LoadMetadata(); err != nil {
			s.initErr = xerrors.Errorf("Can't load metadata: %w", err)
			return
		}
		if err := s.databaseSchema.LoadTablesFromConfig(); err != nil {
			s.initErr = xerrors.Errorf("Can't load tables from config: %w", err)
			return
		}
		if s.tracker != nil {
			if err := s.tracker.Init(); err != nil {
				s.initErr = xerrors.Errorf("Can't init tracker: %w", err)
				return
			}
		}
	})
	return s.initErr
}

func (s *OracleStorage) Ping() error {
	return s.sqlxDB.Ping()
}

func (s *OracleStorage) Close() {
	_ = s.sqlxDB.Close()
}

func (s *OracleStorage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	if err := s.ensureInit(); err != nil {
		return nil, xerrors.Errorf("init: %w", err)
	}
	fullMap, err := oracle_snapshot.NewOracleDataObjects(s.databaseSchema).ToOldTableMap()
	if err != nil {
		return nil, xerrors.Errorf("build table map: %w", err)
	}
	if filter == nil {
		return fullMap, nil
	}
	out := make(abstract.TableMap)
	for id, info := range fullMap {
		if filter.Include(id) {
			out[id] = info
		}
	}
	return out, nil
}

func (s *OracleStorage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	if err := s.ensureInit(); err != nil {
		return nil, xerrors.Errorf("init: %w", err)
	}
	ot, err := s.oracleTableByID(table)
	if err != nil {
		return nil, xerrors.Errorf("find table: %w", err)
	}
	schema, err := ot.ToOldTable()
	if err != nil {
		return nil, xerrors.Errorf("convert schema: %w", err)
	}
	return schema, nil
}

func (s *OracleStorage) TableExists(table abstract.TableID) (bool, error) {
	if err := s.ensureInit(); err != nil {
		return false, err
	}
	_, err := s.oracleTableByID(table)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (s *OracleStorage) oracleTableByID(table abstract.TableID) (*oracle_schema.Table, error) {
	schema := s.databaseSchema.OracleSchemaByName(table.Namespace)
	if schema == nil {
		return nil, xerrors.Errorf("Cannot find schema '%v'", table.Namespace)
	}
	ot := schema.OracleTableByName(table.Name)
	if ot == nil {
		return nil, xerrors.Errorf("Cannot find table '%v.%v'", table.Namespace, table.Name)
	}
	return ot, nil
}

func (s *OracleStorage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	if err := s.ensureInit(); err != nil {
		return 0, xerrors.Errorf("init: %w", err)
	}
	ot, err := s.oracleTableByID(table)
	if err != nil {
		return 0, xerrors.Errorf("find table: %w", err)
	}
	var cnt uint64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", ot.OracleSQLName())
	queryErr := oracle_common.PDBQueryGlobal(&s.config, s.sqlxDB, context.Background(),
		func(ctx context.Context, connection *sqlx.Conn) error {
			return connection.GetContext(ctx, &cnt, q)
		})
	if queryErr != nil {
		return 0, xerrors.Errorf("count rows: %w", queryErr)
	}
	return cnt, nil
}

func (s *OracleStorage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, nil
}

func (s *OracleStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if err := s.ensureInit(); err != nil {
		return xerrors.Errorf("init: %w", err)
	}
	schema := s.databaseSchema.OracleSchemaByName(table.Schema)
	if schema == nil {
		return xerrors.Errorf("Cannot find schema '%v'", table.Schema)
	}
	ot := schema.OracleTableByName(table.Name)
	if ot == nil {
		return xerrors.Errorf("Cannot find table '%v.%v'", table.Schema, table.Name)
	}

	var position *oracle_common.LogPosition
	var err error
	if s.shardedSCN > 0 {
		// Secondary worker: use the SCN fixed by the main worker for consistency.
		position, err = oracle_common.NewLogPosition(s.shardedSCN, nil, nil, oracle_common.PositionSnapshotStarted, time.Now())
		if err != nil {
			return xerrors.Errorf("cannot make sharded log position: %w", err)
		}
	} else if s.tracker != nil {
		position, err = s.tracker.ReadPosition()
		if err != nil {
			return xerrors.Errorf("Can't read current SCN: %w", err)
		}
	} else {
		position, err = oracle_common.NewLogPosition(0, nil, nil, oracle_common.PositionSnapshotStarted, time.Now())
		if err != nil {
			return xerrors.Errorf("cannot make current log position: %w", err)
		}
	}

	filter := table.Filter

	tsrc, err := oracle_snapshot.NewTableSource(s.sqlxDB, &s.config, position, ot, s.logger, s.sourceStats, filter)
	if err != nil {
		return xerrors.Errorf("Can't create table source: %w", err)
	}
	if err := tsrc.Load(ctx, pusher); err != nil {
		return xerrors.Errorf("load '%v': %w", ot.OracleSQLName(), err)
	}
	return nil
}
