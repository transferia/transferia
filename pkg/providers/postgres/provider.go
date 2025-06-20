//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
	abstract_sink "github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.PgSource", new(PgSource))
	gobwrapper.RegisterName("*server.PgDestination", new(PgDestination))
	model.RegisterDestination(ProviderType, func() model.Destination {
		return new(PgDestination)
	})
	model.RegisterSource(ProviderType, func() model.Source {
		return new(PgSource)
	})

	abstract.RegisterProviderName(ProviderType, "PostgreSQL")
	providers.Register(ProviderType, New)

	/*
		"__consumer_keeper":
			consumer TEXT, locked_till TIMESTAMPTZ, locked_by TEXT
			Table in which we regularly write something. The fact of writing is important here, not the data itself.
			The problem that we solve is if there is no record in the cluster, then we do not receive any events
			from the slot, and we do not commit any progress of reading replication slot.
			And if we don't commit any progress, then WAL accumulates.

		"__data_transfer_lsn":
			transfer_id TEXT, schema_name TEXT, table_name TEXT, lsn BIGINT
			Table (in target) needed for resolving data overlapping during SNAPSHOT_AND_INCREMENT transfers.
	*/
	abstract.RegisterSystemTables(TableConsumerKeeper, TableLSN, dblog.SignalTableName)
}

const (
	TableConsumerKeeper = abstract.TableConsumerKeeper // "__consumer_keeper"
	TableLSN            = abstract.TableLSN            // "__data_transfer_lsn"
)

// To verify providers contract implementation
var (
	_ providers.Sampleable  = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)
	_ providers.Verifier    = (*Provider)(nil)
	_ providers.Activator   = (*Provider)(nil)
	_ providers.Deactivator = (*Provider)(nil)
	_ providers.Cleanuper   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Cleanup(ctx context.Context, task *model.TransferOperation) error {
	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return xerrors.Errorf("error getting src params from transfer: %w", err)
	}
	if p.transfer.SnapshotOnly() {
		return nil
	}
	tracker := NewTracker(p.transfer.ID, p.cp)
	if err := DropReplicationSlot(src, tracker); err != nil {
		return xerrors.Errorf("Unable to drop replication slot: %w", err)
	}
	if src.DBLogEnabled {
		if err := p.DBLogCleanup(ctx, src); err != nil {
			return xerrors.Errorf("unable to cleanup dblog resourses")
		}
	}
	return nil
}

func (p *Provider) Deactivate(ctx context.Context, task *model.TransferOperation) error {
	if p.transfer.SnapshotOnly() {
		return nil
	}

	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return xerrors.Errorf("error getting src params from transfer: %w", err)
	}

	tracker := NewTracker(p.transfer.ID, p.cp)
	if err := DropReplicationSlot(src, tracker); err != nil {
		return xerrors.Errorf("Unable to drop replication slot: %w", err)
	}

	if !p.transfer.IncrementOnly() && src.PostSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPostSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply pre-steps to transfer schema: %w", err)
		}
	}
	return nil
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return xerrors.Errorf("error getting src params from transfer: %w", err)
	}
	if err := VerifyPostgresTables(src, p.transfer, p.logger); err != nil {
		if IsPKeyCheckError(err) {
			if !p.transfer.SnapshotOnly() {
				return xerrors.Errorf("some tables have no PRIMARY KEY. This is allowed for Snapshot-only transfers. Error: %w", err)
			}
			logger.Log.Warn("Some tables have no PRIMARY KEY. This is allowed for Snapshot-only transfers", log.Error(err))
		} else {
			return xerrors.Errorf("tables verification failed: %w", err)
		}
	}
	p.logger.Info("Preparing PostgreSQL source")
	tracker := NewTracker(p.transfer.ID, p.cp)
	if src.DBLogEnabled && !p.transfer.IncrementOnly() { // if there are present SNAPSHOT stage with turned-on DBLog
		if err := p.DBLogCreateSlotAndInit(ctx, tracker); err != nil {
			return xerrors.Errorf("unable to init dblog, err: %w", err)
		}
		callbacks.Rollbacks.Add(func() {
			if err := DropReplicationSlot(src, tracker); err != nil {
				logger.Log.Error("Unable to drop replication slot", log.Error(err), log.String("slot_name", src.SlotID))
			}
		})
	} else if !p.transfer.SnapshotOnly() { // if there are present REPLICATION stage
		if err := CreateReplicationSlot(src, tracker); err != nil {
			return xerrors.Errorf("failed to create a replication slot %q at source: %w", src.SlotID, err)
		}
		callbacks.Rollbacks.Add(func() {
			if err := DropReplicationSlot(src, tracker); err != nil {
				logger.Log.Error("Unable to drop replication slot", log.Error(err), log.String("slot_name", src.SlotID))
			}
		})
	}

	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("failed to cleanup sink: %w", err)
		}
	}
	if src.PreSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPreSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply pre-steps to transfer schema: %w", err)
		}
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}

		if src.DBLogEnabled {
			logger.Log.Info("DBLog enabled")
			if err := p.DBLogUpload(ctx, tables); err != nil {
				return xerrors.Errorf("DBLog snapshot loading failed: %w", err)
			}
		} else {
			if err := callbacks.Upload(tables); err != nil {
				return xerrors.Errorf("Snapshot loading failed: %w", err)
			}
		}
	}
	if p.transfer.SnapshotOnly() && src.PostSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPostSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply post-steps to transfer schema: %w", err)
		}
	}
	return nil
}

func (p *Provider) Verify(ctx context.Context) error {
	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return xerrors.Errorf("error getting src snapshot sink params from transfer: %w", err)
	}
	if src.SubNetworkID != "" {
		return xerrors.New("unable to verify derived network")
	}
	if err := VerifyPostgresTables(src, p.transfer, p.logger); err != nil {
		if IsPKeyCheckError(err) && p.transfer.SnapshotOnly() {
			logger.Log.Warnf("Some tables dont have primary key but it is still allowed for snapshot only transfers: %v", err)
		} else {
			return xerrors.Errorf("unable to verify postgres tables: %w", err)
		}
	}
	if !p.transfer.SnapshotOnly() {
		tracker := NewTracker(p.transfer.ID, p.cp)
		if err := CreateReplicationSlot(src, tracker); err == nil {
			if err := DropReplicationSlot(src, tracker); err != nil {
				return xerrors.Errorf("unable to drop replication slot: %w", err)
			}
		} else {
			return xerrors.Errorf("unable to create replication slot: %w", err)
		}
	}
	return nil
}

func (p *Provider) dstParamsFromTransfer() (*PgDestination, error) {
	transferDst, ok := p.transfer.Dst.(*PgDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Dst)
	}
	// prevent accidental transfer params mutation as it may break tests
	dst := *transferDst
	if p.transfer.SrcType() != ProviderType {
		dst.MaintainTables = true
	}
	dst.CopyUpload = false
	return &dst, nil
}

func (p *Provider) Sink(mwConfig middlewares.Config) (abstract.Sinker, error) {
	dst, err := p.dstParamsFromTransfer()
	if err != nil {
		return nil, xerrors.Errorf("error getting dst sink params from transfer: %w", err)
	}
	return NewSink(p.logger, p.transfer.ID, dst.ToSinkParams(), p.registry)
}

func (p *Provider) SnapshotSink(mwConfig middlewares.Config) (abstract.Sinker, error) {
	dst, err := p.dstParamsFromTransfer()
	if err != nil {
		return nil, xerrors.Errorf("error getting dst snapshot sink params from transfer: %w", err)
	}
	if p.transfer.SrcType() == ProviderType {
		dst.CopyUpload = true
	}
	dst.PerTransactionPush = false // should be always disabled for snapshot stage
	return NewSink(p.logger, p.transfer.ID, dst.ToSinkParams(), p.registry)
}

func (p *Provider) Source() (abstract.Source, error) {
	s, err := p.srcParamsFromTransfer()
	if err != nil {
		return nil, xerrors.Errorf("error getting source params from transfer: %w", err)
	}
	st := stats.NewSourceStats(p.registry)
	return backoff.RetryNotifyWithData(func() (abstract.Source, error) {
		return NewSourceWrapper(s, p.transfer.ID, p.transfer.DataObjects, p.logger, st, p.cp, false)
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 3), util.BackoffLoggerWarn(p.logger, "unable to init pg source"))
}

// Build a type mapping and print elapsed time in log.
func buildTypeMapping(ctx context.Context, storage *Storage) (TypeNameToOIDMap, error) {
	startTime := time.Now()
	logger.Log.Info("Building type map for the destination database")
	typeMapping, err := storage.BuildTypeMapping(ctx)

	logger.Log.Info("Built type map for the destination database", log.Duration("elapsed", time.Since(startTime)), log.Int("entries_count", len(typeMapping)), log.Error(err))
	return typeMapping, err
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return nil, xerrors.Errorf("error getting src storage params from transfer: %w", err)
	}
	opts := []StorageOpt{WithMetrics(p.registry)}
	pgDst, ok := p.transfer.Dst.(*PgDestination)
	if ok {
		dstStorage, err := NewStorage(pgDst.ToStorageParams())
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to connect to the destination cluster to get type information: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		typeMap, err := buildTypeMapping(ctx, dstStorage)
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to build destination database type mapping: %w", err)
		}
		opts = append(opts, WithTypeMapping(typeMap))
	}
	storage, err := NewStorage(src.ToStorageParams(p.transfer), opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a PostgreSQL storage: %w", err)
	}
	storage.IsHomo = src.IsHomo
	if p.transfer.DataObjects != nil && len(p.transfer.DataObjects.IncludeObjects) > 0 {
		storage.loadDescending = src.CollapseInheritTables // For include objects we force to load parent table with all their children
	}
	return storage, nil
}

func (p *Provider) srcParamsFromTransfer() (*PgSource, error) {
	transferSrc, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	// prevent accidental transfer params mutation as it may break tests
	src := *transferSrc
	if src.SlotID == "" {
		src.SlotID = p.transfer.ID
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType
	}
	if src.NoHomo {
		src.IsHomo = false
	}
	return &src, nil
}

func (p *Provider) SourceSampleableStorage() (abstract.SampleableStorage, []abstract.TableDescription, error) {
	src, err := p.srcParamsFromTransfer()
	if err != nil {
		return nil, nil, xerrors.Errorf("error getting src sampleable storage params from transfer: %w", err)
	}
	srcStorage, err := NewStorage(src.ToStorageParams(p.transfer))
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to create pg storage: %w`, err)
	}
	defer srcStorage.Close()
	if _, ok := p.transfer.Dst.(*PgDestination); ok {
		srcStorage.IsHomo = true // Need to toggle is homo to exclude view and mat views
	}
	all, err := srcStorage.TableList(nil)
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to get table map from source storage: %w`, err)
	}
	var tables []abstract.TableDescription
	for tID, tInfo := range all {
		if tID.Name == TableConsumerKeeper || tID.Name == dblog.SignalTableName {
			continue
		}
		if src.Include(tID) {
			tables = append(tables, abstract.TableDescription{
				Name:   tID.Name,
				Schema: tID.Namespace,
				Filter: "",
				EtaRow: tInfo.EtaRow,
				Offset: 0,
			})
		}
	}
	return srcStorage, tables, nil
}

func (p *Provider) DestinationSampleableStorage() (abstract.SampleableStorage, error) {
	dst, ok := p.transfer.Dst.(*PgDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	return NewStorage(dst.ToStorageParams())
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) DBLogCreateSlotAndInit(ctx context.Context, tracker *Tracker) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}

	exists, err := CreateReplicationSlotIfNotExists(src, tracker)
	if err != nil {
		return xerrors.Errorf("failed to create a replication slot (1) %q at source: %w", src.SlotID, err)
	}

	pgStorage, err := NewStorage(src.ToStorageParams(p.transfer))
	if err != nil {
		return xerrors.Errorf("failed to create postgres storage: %w", err)
	}
	// ensure SignalTable exists
	_, err = dblog.NewPgSignalTable(ctx, pgStorage.Conn, logger.Log, p.transfer.ID, src.KeeperSchema)
	if err != nil {
		return xerrors.Errorf("unable to create signal table: %w", err)
	}
	if !exists {
		// delete previous watermarks - only if slot previously not existed. It existed - it's just dataplane restart
		if err := dblog.DeleteWatermarks(ctx, pgStorage.Conn, src.KeeperSchema, p.transfer.ID); err != nil {
			return xerrors.Errorf("unable to delete watermarks: %w", err)
		}
	}
	return nil
}

func (p *Provider) DBLogUpload(ctx context.Context, tables abstract.TableMap) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	pgStorage, err := NewStorage(src.ToStorageParams(p.transfer))
	if err != nil {
		return xerrors.Errorf("failed to create postgres storage: %w", err)
	}

	tableDescs := tables.ConvertToTableDescriptions()
	for _, table := range tableDescs {
		asyncSink, err := abstract_sink.MakeAsyncSink(
			p.transfer,
			logger.Log,
			p.registry,
			p.cp,
			middlewares.MakeConfig(middlewares.WithEnableRetries),
		)

		pusher := abstract.PusherFromAsyncSink(asyncSink)

		if err != nil {
			return xerrors.Errorf("failed to make async sink: %w", err)
		}

		if err = backoff.RetryNotify(func() error {
			logger.Log.Infof("Starting upload table: %s", table.String())

			sourceWrapper, err := NewSourceWrapper(src, src.SlotID, p.transfer.DataObjects, p.logger, stats.NewSourceStats(p.registry), p.cp, true)
			if err != nil {
				return xerrors.Errorf("failed to create source wrapper: %w", err)
			}

			dblogStorage, err := dblog.NewStorage(p.logger, sourceWrapper, pgStorage, pgStorage.Conn, src.ChunkSize, p.transfer.ID, src.KeeperSchema, Represent)
			if err != nil {
				return xerrors.Errorf("failed to create DBLog storage: %w", err)
			}

			err = dblogStorage.LoadTable(ctx, table, pusher)
			if abstract.IsFatal(err) {
				return backoff.Permanent(xerrors.Errorf("fatal error ocurred in dblogStorage.LoadTable, err: %w", err))
			} else if err != nil {
				return xerrors.Errorf("unable to dblogStorage.LoadTable, err: %w", err)
			}
			logger.Log.Infof("Upload table %s successfully", table.String())
			return nil
		}, util.NewExponentialBackOff(), util.BackoffLogger(logger.Log, fmt.Sprintf("loading table: %s", table.String()))); err != nil {
			return xerrors.Errorf("failed to load table: %w", err)
		}
	}

	return nil
}

func (p *Provider) DBLogCleanup(ctx context.Context, src *PgSource) error {
	conn, err := MakeConnPoolFromSrc(src, p.logger)
	if err != nil {
		return xerrors.Errorf("failed to create a connection pool: %w", err)
	}
	defer conn.Close()

	return dblog.DeleteWatermarks(ctx, conn, src.KeeperSchema, p.transfer.ID)
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
