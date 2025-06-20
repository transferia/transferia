//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const tableIsShardedKey = "Offset column used as worker index"

type (
	checkConnectionFunc func(ctx context.Context, pgs *postgres.Storage, expectedSP GPSegPointer) error
	newFlavorFunc       func(in *Storage) postgres.DBFlavour
)

type Storage struct {
	// config is NOT read-only and can change during execution
	config      *GpSource
	sourceStats *stats.SourceStats

	postgreses mutexedPostgreses

	coordinatorTx   *gpTx
	livenessMonitor *livenessMonitor

	workersCount int
	schemas      map[abstract.TableID]*abstract.TableSchema

	shardedState *WorkersGpConfig

	checkConnection checkConnectionFunc
	newFlavor       newFlavorFunc
}

func defaultNewFlavor(in *Storage) postgres.DBFlavour {
	return NewGreenplumFlavour(in.workersCount == 1)
}

func NewStorageImpl(config *GpSource, mRegistry metrics.Registry, checkConnection checkConnectionFunc, newFlavor newFlavorFunc) *Storage {
	return &Storage{
		config:      config,
		sourceStats: stats.NewSourceStats(mRegistry),

		postgreses: newMutexedPostgreses(),

		coordinatorTx:   nil,
		livenessMonitor: nil,

		workersCount: 1,
		schemas:      make(map[abstract.TableID]*abstract.TableSchema),

		shardedState: nil,

		checkConnection: checkConnection,
		newFlavor:       newFlavor,
	}
}

func NewStorage(config *GpSource, mRegistry metrics.Registry) *Storage {
	return NewStorageImpl(config, mRegistry, checkConnection, defaultNewFlavor)
}

const PingTimeout = 5 * time.Minute

func (s *Storage) Close() {
	if s.coordinatorTx != nil {
		ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
		defer cancel()
		if err := s.coordinatorTx.CloseRollback(ctx); err != nil {
			logger.Log.Warn("Failed to rollback transaction in Greenplum", log.Error(err))
		}
		s.coordinatorTx = nil
	}
	s.postgreses.Close()
}

func (s *Storage) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("Greenplum is unavailable: %w", err)
	}

	return storage.Ping()
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to Greenplum %s: %w", Coordinator().String(), err)
	}

	return storage.TableSchema(ctx, table)
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if s.workersCount == 1 || table.Filter != tableIsShardedKey {
		logger.Log.Info("Loading table in non-distributed mode", log.String("table", table.Fqtn()))
		if err := s.LoadTableImplNonDistributed(ctx, table, pusher); err != nil {
			return xerrors.Errorf("failed to load table in non-distributed mode: %w", err)
		}
		logger.Log.Info("Successfully loaded table in non-distributed mode", log.String("table", table.Fqtn()))
		return nil
	}

	logger.Log.Info("Loading table in distributed mode (using utility mode connections)", log.String("table", table.Fqtn()))
	if err := s.LoadTableImplDistributed(ctx, table, pusher); err != nil {
		return xerrors.Errorf("failed to load table in distributed mode: %w", err)
	}
	logger.Log.Info("Successfully loaded table in distributed mode", log.String("table", table.Fqtn()))
	return nil
}

func (s *Storage) LoadTableImplNonDistributed(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if table.Filter == tableIsShardedKey {
		// clear sharding info
		table.Filter = ""
		table.Offset = 0
	}
	if err := s.ensureCoordinatorTx(ctx); err != nil {
		return xerrors.Errorf("failed to start a transaction on Greenplum %s: %w", Coordinator().String(), err)
	}
	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("failed to connect to Greenplum %s: %w", Coordinator().String(), err)
	}
	if err := s.segmentLoadTable(ctx, storage, s.coordinatorTx, table, pusher); err != nil {
		return xerrors.Errorf("failed to load table from Greenplum %s: %w", Coordinator().String(), err)
	}
	return nil
}

func GpHAPFromGreenplumAPIHAPair(hap *GreenplumHAPair) *GpHAP {
	var mirror *GpHP
	if hap.GetMirror() != nil {
		mirror = &GpHP{
			hap.GetMirror().GetHost(),
			int(hap.GetMirror().GetPort()),
		}
	}

	pair := &GpHAP{
		Primary: &GpHP{
			hap.GetPrimary().GetHost(),
			int(hap.GetPrimary().GetPort()),
		},
		Mirror: mirror,
	}
	return pair
}

func GpClusterFromGreenplumCluster(c *GreenplumCluster) *GpCluster {
	segments := make([]*GpHAP, len(c.GetSegments()))
	for i, pair := range c.GetSegments() {
		segments[i] = GpHAPFromGreenplumAPIHAPair(pair)
	}

	cluster := &GpCluster{
		Coordinator: GpHAPFromGreenplumAPIHAPair(c.GetCoordinator()),
		Segments:    segments,
	}

	return cluster
}

func (s *Storage) LoadTableImplDistributed(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if table.Filter != tableIsShardedKey {
		return abstract.NewFatalError(xerrors.New("Table is not sharded"))
	}

	workerID := int32(table.Offset)
	// clear sharding info
	table.Filter = ""
	table.Offset = 0

	if s.shardedState == nil {
		return abstract.NewFatalError(xerrors.New("gpConfig is missing from sharded state"))
	}

	if s.config.Connection.MDBCluster != nil {
		// override connection properties with information retrieved by main worker
		s.config.Connection.OnPremises = GpClusterFromGreenplumCluster(s.shardedState.GetCluster())
	}

	thisWorkerRule := s.shardedState.GetWtsList()[workerID-1]
	if thisWorkerRule.GetWorkerID() != workerID {
		return abstract.NewFatalError(xerrors.Errorf("worker ID in the sharding configuration (%d) does not match the runtime worker ID (%d)", thisWorkerRule.GetWorkerID(), workerID))
	}

	segAndXIDs := thisWorkerRule.GetSegments()
	logger.Log.Debug("Loading table in distributed mode from assigned segments", log.String("table", table.Fqtn()), log.Array("segments", segAndXIDs))
	for _, segAndXID := range segAndXIDs {
		seg := Segment(int(segAndXID.GetSegmentID()))
		err := backoff.RetryNotify(
			func() error {
				storage, err := s.PGStorage(ctx, seg)
				if err != nil {
					return xerrors.Errorf("failed to connect: %w", err)
				}
				tx, err := newGpTx(ctx, storage)
				if err != nil {
					return xerrors.Errorf("failed to BEGIN transaction: %w", err)
				}
				if err := s.segmentLoadTable(ctx, storage, tx, table, pusher); err != nil {
					if err := tx.CloseRollback(ctx); err != nil {
						logger.Log.Warn("Failed to ROLLBACK transaction", log.String("table", table.Fqtn()), log.String("segment", seg.String()), log.Error(err))
					}
					return xerrors.Errorf("failed to load table: %w", err)
				}
				if err := tx.CloseCommit(ctx); err != nil {
					return xerrors.Errorf("failed to COMMIT transaction: %w", err)
				}
				return nil
			},
			// Greenplum segments must recover in milliseconds, so 1s backoff is fine
			backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3),
			util.BackoffLogger(logger.Log, fmt.Sprintf("load table %s from Greenplum %s by worker %d", table.Fqtn(), seg.String(), workerID)),
		)
		if err != nil {
			// If we are here, both segment and mirror are unavailable.
			// The whole transfer must fail. Otherwise we return "success", although some data was not transferred.
			// This breaks the guarantee we provide for strong snapshot consistency disabled.
			return xerrors.Errorf("Greenplum snapshot failed while loading table %s from %s by worker %d: %w", table.Fqtn(), seg.String(), workerID, err)
		}
		logger.Log.Info("Successfully loaded a chunk of data from Greenplum", log.String("table", table.Fqtn()), log.String("segment", seg.String()), log.Int32("worker", workerID))
	}

	return nil
}

func (s *Storage) segmentLoadTable(ctx context.Context, storage *postgres.Storage, tx *gpTx, table abstract.TableDescription, pusher abstract.Pusher) error {
	s.sourceStats.Count.Inc()

	err := tx.withConnection(func(conn *pgx.Conn) error {
		schema, err := s.schemaForTable(ctx, storage, conn, table)
		if err != nil {
			return xerrors.Errorf("schema for table %s not found: %w", table.Fqtn(), err)
		}

		readQuery := storage.OrderedRead(&table, schema.Columns(), postgres.SortAsc, abstract.NoFilter, postgres.All, false)
		rows, err := conn.Query(ctx, readQuery, pgx.QueryResultFormats{pgx.BinaryFormatCode})
		if err != nil {
			logger.Log.Error("Failed to execute SELECT", log.String("table", table.Fqtn()), log.String("query", readQuery), log.Error(err))
			return xerrors.Errorf("failed to execute SELECT: %w", err)
		}
		defer rows.Close()

		ciFetcher := postgres.NewChangeItemsFetcher(rows, conn, abstract.ChangeItem{
			ID:           uint32(0),
			LSN:          uint64(0),
			CommitTime:   uint64(time.Now().UTC().UnixNano()),
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       table.Schema,
			Table:        table.Name,
			PartID:       table.PartID(),
			ColumnNames:  schema.Columns().ColumnNames(),
			ColumnValues: nil,
			TableSchema:  schema,
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.EmptyEventSize(),
		}, s.sourceStats)

		totalRowsRead := uint64(0)

		logger.Log.Info("Sink uploading table", log.String("fqtn", table.Fqtn()))

		for ciFetcher.MaybeHasMore() {
			items, err := ciFetcher.Fetch()
			if err != nil {
				return xerrors.Errorf("failed to extract data from table %s: %w", table.Fqtn(), err)
			}
			if len(items) > 0 {
				totalRowsRead += uint64(len(items))
				s.sourceStats.ChangeItems.Add(int64(len(items)))
				if err := pusher(items); err != nil {
					return xerrors.Errorf("failed to push %d ChangeItems. Error: %w", len(items), err)
				}
			}
		}

		return nil
	})
	return err
}

func (s *Storage) schemaForTable(ctx context.Context, storage *postgres.Storage, conn *pgx.Conn, table abstract.TableDescription) (*abstract.TableSchema, error) {
	if _, ok := s.schemas[table.ID()]; !ok {
		loaded, err := storage.LoadSchemaForTable(ctx, conn, table)
		if err != nil {
			return nil, xerrors.Errorf("failed to load schema for table %s: %w", table.Fqtn(), err)
		}
		s.schemas[table.ID()] = loaded
	}
	return s.schemas[table.ID()], nil
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return nil, xerrors.Errorf("Greenplum is unavailable: %w", err)
	}

	result, err := storage.TableList(filter)
	if err != nil {
		return nil, xerrors.Errorf("failed to list tables on Greenplum %s: %w", Coordinator(), err)
	}
	return result, nil
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return 0, xerrors.Errorf("Greenplum is unavailable: %w", err)
	}

	return storage.ExactTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return 0, xerrors.Errorf("Greenplum is unavailable: %w", err)
	}

	return storage.EstimateTableRowsCount(table)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return false, xerrors.Errorf("Greenplum is unavailable: %w", err)
	}

	return storage.TableExists(table)
}

// ShardTable implements ShardingStorage by replicating the table, producing the number of tables equal to the number of jobs.
// This approach is taken because Greenplum shards load by segments, stored in context; not by tables.
func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	if table.Filter != "" || table.Offset != 0 {
		logger.Log.Infof("Table %v will not be sharded, filter: [%v], offset: %v", table.Fqtn(), table.Filter, table.Offset)
		return []abstract.TableDescription{table}, nil
	}
	result := make([]abstract.TableDescription, s.workersCount)
	for i := 0; i < s.workersCount; i++ {
		result[i] = table
		// See https://st.yandex-team.ru/TM-6811
		result[i].Filter = tableIsShardedKey
		result[i].Offset = uint64(i + 1) // Use as worker index
		result[i].EtaRow = EtaRowPartialProgress
	}
	return result, nil
}

func (s *Storage) ShardingContext() ([]byte, error) {
	jsonctx, err := json.Marshal(s.WorkersGpConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal gp config: %w", err)
	}
	return jsonctx, nil
}

func (s *Storage) SetShardingContext(shardedState []byte) error {
	res := new(WorkersGpConfig)
	if err := json.Unmarshal(shardedState, res); err != nil {
		return xerrors.Errorf("unable to restore sharding state back to proto: %w", err)
	}
	s.shardedState = res
	if s.shardedState == nil {
		return abstract.NewFatalError(xerrors.New("gpConfig is missing from sharded state"))
	}
	return nil
}

// Named BeginGPSnapshot to NOT match abstract.SnapshotableStorage;
// BeginGPSnapshot starts a Greenplum cluster-global transaction;
func (s *Storage) BeginGPSnapshot(ctx context.Context, tables []abstract.TableDescription) error {
	if err := s.ensureCoordinatorTx(ctx); err != nil {
		return xerrors.Errorf("failed to start a transaction on Greenplum %s: %w", Coordinator().String(), err)
	}

	if s.workersCount > 1 {
		// sharded transfer requires table locking, otherwise it will not be consistent
		lockMode := postgres.AccessShareLockMode
		if s.config.AdvancedProps.EnforceConsistency {
			lockMode = postgres.ShareLockMode
		}
		for _, t := range tables {
			err := s.coordinatorTx.withConnection(func(conn *pgx.Conn) error {
				logger.Log.Info("Locking table", log.String("table", t.Fqtn()), log.String("mode", string(lockMode)))
				_, err := conn.Exec(ctx, postgres.LockQuery(t.ID(), lockMode))
				return err
			})
			if err != nil {
				return xerrors.Errorf("failed to lock table %s in %s mode: %w", t.Fqtn(), string(lockMode), err)
			}
		}
	}

	if s.workersCount > 1 && !s.config.AdvancedProps.AllowCoordinatorTxFailure {
		// monitor must only be run in sharded transfer and disabled when coordinator TX failures are tolerated
		if s.config.AdvancedProps.LivenessMonitorCheckInterval <= 0 {
			s.config.AdvancedProps.LivenessMonitorCheckInterval = 30 * time.Second
		}
		s.livenessMonitor = newLivenessMonitor(s.coordinatorTx, ctx, s.config.AdvancedProps.LivenessMonitorCheckInterval)
	}

	return nil
}

func (s *Storage) ensureCoordinatorTx(ctx context.Context) error {
	if s.coordinatorTx != nil {
		return nil
	}
	storage, err := s.PGStorage(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("Greenplum is unavailable: %w", err)
	}
	tx, err := newGpTx(ctx, storage)
	if err != nil {
		return xerrors.Errorf("failed to start a transaction: %w", err)
	}
	s.coordinatorTx = tx
	return nil
}

// Named EndGPSnapshot to NOT match abstract.SnapshotableStorage;
// EndGPSnapshot ceases a Greenplum cluster-global transaction;
func (s *Storage) EndGPSnapshot(ctx context.Context) error {
	s.livenessMonitor.Close()

	if s.coordinatorTx == nil {
		return nil
	}
	defer func() {
		s.coordinatorTx = nil
	}()

	if err := s.coordinatorTx.CloseCommit(ctx); err != nil {
		if !s.config.AdvancedProps.AllowCoordinatorTxFailure {
			return xerrors.Errorf("failed to end snapshot: %w", err)
		}
		logger.Log.Warn("coordinator transaction failed", log.Error(err))
	}
	return nil
}

func (s *Storage) WorkersCount() int {
	return s.workersCount
}

func (s *Storage) SetWorkersCount(count int) {
	s.workersCount = count
}

func GreenplumAPIHAPairFromGpHAP(hap *GpHAP) *GreenplumHAPair {
	var mirror *GreenplumHostPort
	if hap.Mirror != nil {
		mirror = &GreenplumHostPort{
			Host: hap.Mirror.Host,
			Port: int64(hap.Mirror.Port),
		}
	}

	pair := &GreenplumHAPair{
		Primary: &GreenplumHostPort{
			Host: hap.Primary.Host,
			Port: int64(hap.Primary.Port),
		},
		Mirror: mirror,
	}
	return pair
}

func GreenplumClusterFromGpCluster(c *GpCluster) *GreenplumCluster {
	if c == nil {
		return nil
	}

	segments := make([]*GreenplumHAPair, len(c.Segments))
	for i, pair := range c.Segments {
		segments[i] = GreenplumAPIHAPairFromGpHAP(pair)
	}

	cluster := &GreenplumCluster{
		Coordinator: GreenplumAPIHAPairFromGpHAP(c.Coordinator),
		Segments:    segments,
	}

	return cluster
}

func (s *Storage) WorkersGpConfig() *WorkersGpConfig {
	return &WorkersGpConfig{
		WtsList: workerToGpSegMapping(len(s.config.Connection.OnPremises.Segments), s.workersCount),
		Cluster: GreenplumClusterFromGpCluster(s.config.Connection.OnPremises),
	}
}

func workerToGpSegMapping(nSegments int, nWorkers int) []*WorkerIDToGpSegs {
	baseSegsPerWorker := nSegments / nWorkers
	workersWithExtraSegment := nSegments % nWorkers

	workerSegPairs := make([]*WorkerIDToGpSegs, nWorkers)
	segI := 0
	for i := range workerSegPairs {
		workerSegPairs[i] = new(WorkerIDToGpSegs)
		pairWorker := workerSegPairs[i]
		pairWorker.WorkerID = int32(i + 1) // workers' numbering starts from 1
		segsForThisWorker := baseSegsPerWorker
		if i < workersWithExtraSegment {
			segsForThisWorker += 1
		}
		pairWorker.Segments = make([]*GpSegAndXID, segsForThisWorker)
		for j := range pairWorker.Segments {
			pairWorker.Segments[j] = new(GpSegAndXID)
			pairSeg := pairWorker.Segments[j]
			pairSeg.SegmentID = int32(segI)
			segI += 1
		}
	}

	return workerSegPairs
}

// RunSlotMonitor in Greenplum returns the liveness monitor. There are no replication slots in Greenplum.
// The liveness monitor ensures the transaction is still open and simple queries can be run on it.
// The liveness monitor is only run in sharded transfers. It starts automatically at BeginSnapshot.
func (s *Storage) RunSlotMonitor(ctx context.Context, serverSource interface{}, registry metrics.Registry) (abstract.SlotKiller, <-chan error, error) {
	if s.livenessMonitor != nil {
		return &abstract.StubSlotKiller{}, s.livenessMonitor.C(), nil
	}

	if !(s.workersCount > 1) {
		return &abstract.StubSlotKiller{}, make(chan error), nil
	}
	return nil, nil, abstract.NewFatalError(xerrors.New("liveness monitor is not running, probably because a snapshot has not begun yet"))
}
