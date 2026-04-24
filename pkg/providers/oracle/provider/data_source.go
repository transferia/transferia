package provider

import (
	"time"

	"github.com/jmoiron/sqlx"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract2"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/providers/oracle/logtracker"
	logminer "github.com/transferia/transferia/pkg/providers/oracle/replication/log_miner"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	oracle_snapshot "github.com/transferia/transferia/pkg/providers/oracle/snapshot"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type OracleDataProvider struct {
	sqlxDB                  *sqlx.DB
	snaphotSplitTransaction *sqlx.Tx
	logger                  log.Logger
	registry                core_metrics.Registry
	sourceStats             *stats.SourceStats
	config                  provider_oracle.OracleSource
	databaseSchema          *oracle_schema.Database
	tracker                 logtracker.LogTracker
}

// To verify providers contract implementation
var (
	_ abstract2.SnapshotProvider = (*OracleDataProvider)(nil)
)

func NewOracleDataProvider(
	logger log.Logger,
	registry core_metrics.Registry,
	controlPlaneClient coordinator.Coordinator,
	config *provider_oracle.OracleSource,
	transferID string,
) (*OracleDataProvider, error) {
	sqlxDB, err := oracle_common.CreateConnection(config)
	if err != nil {
		return nil, xerrors.Errorf("Can't create connection: %w", err)
	}

	schemaRepo, err := oracle_schema.NewDatabase(sqlxDB, config, logger)
	if err != nil {
		return nil, xerrors.Errorf("Can't create schema repository: %w", err)
	}

	tracker, err := createLogTracker(sqlxDB, controlPlaneClient, config, transferID)
	if err != nil {
		return nil, xerrors.Errorf("Can't create log tracker: %w", err)
	}

	dataSource := &OracleDataProvider{
		sqlxDB:                  sqlxDB,
		snaphotSplitTransaction: nil,
		logger:                  logger,
		registry:                registry,
		sourceStats:             stats.NewSourceStats(registry),
		config:                  *config,
		databaseSchema:          schemaRepo,
		tracker:                 tracker,
	}

	return dataSource, nil
}

func createLogTracker(
	sqlxDB *sqlx.DB,
	controlPlaneClient coordinator.Coordinator,
	config *provider_oracle.OracleSource,
	transferID string,
) (logtracker.LogTracker, error) {
	switch config.TrackerType {
	case provider_oracle.OracleNoLogTracker:
		return nil, nil
	case provider_oracle.OracleInMemoryLogTracker:
		inMemoryLogTracker, err := logtracker.NewInMemoryLogTracker(transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create in memory log tracker: %w", err)
		}
		return inMemoryLogTracker, nil
	case provider_oracle.OracleEmbeddedLogTracker:
		embeddedLogTracker, err := logtracker.NewEmbeddedLogTracker(sqlxDB, config, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create embedded log tracker: %w", err)
		}
		return embeddedLogTracker, nil
	case provider_oracle.OracleInternalLogTracker:
		internalLogTracker, err := logtracker.NewInternalLogTracker(controlPlaneClient, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create embedded log tracker: %w", err)
		}
		return internalLogTracker, nil
	default:
		return nil, xerrors.Errorf("Unknown tracker type '%v'", config.TrackerType)
	}
}

func (data *OracleDataProvider) DatabaseSchema() (*oracle_schema.Database, error) {
	return data.databaseSchema, nil
}

func (data *OracleDataProvider) Tracker() (logtracker.LogTracker, error) {
	return data.tracker, nil
}

func (data *OracleDataProvider) getCurrentLogPosition(transferType abstract.TransferType) (*oracle_common.LogPosition, error) {
	sql := "SELECT CURRENT_SCN, sys_extract_utc(systimestamp) as CURRENT_TIMESTAMP FROM v$database"
	var sqlResult struct {
		SCN              uint64    `db:"CURRENT_SCN"`
		CurrentTimestamp time.Time `db:"CURRENT_TIMESTAMP"`
	}
	if err := data.sqlxDB.Get(&sqlResult, sql); err != nil {
		return nil, xerrors.Errorf("Can't select current SCN from DB: %w", err)
	}

	var positionType oracle_common.PositionType
	switch transferType {
	case abstract.TransferTypeSnapshotOnly, abstract.TransferTypeSnapshotAndIncrement:
		positionType = oracle_common.PositionSnapshotStarted
	case abstract.TransferTypeIncrementOnly:
		positionType = oracle_common.PositionReplication
	default:
		return nil, xerrors.Errorf("Transfer type '%v' is not supported", transferType)
	}

	return oracle_common.NewLogPosition(sqlResult.SCN, nil, nil, positionType, sqlResult.CurrentTimestamp)
}

// Begin of base DataProvider interface

func (data *OracleDataProvider) Init() error {
	if err := data.databaseSchema.LoadMetadata(); err != nil {
		return xerrors.Errorf("Can't init data source, can't load metadata: %w", err)
	}

	if err := data.databaseSchema.LoadTablesFromConfig(); err != nil {
		return xerrors.Errorf("Can't init data source, can't load schema: %w", err)
	}

	if data.tracker != nil {
		if err := data.tracker.Init(); err != nil {
			return xerrors.Errorf("Can't init data source, can't init tracker: %w", err)
		}
	}

	return nil
}

func (data *OracleDataProvider) Ping() error {
	if err := data.sqlxDB.Ping(); err != nil {
		return xerrors.Errorf("Can't ping DB: %w", err)
	}
	return nil
}

func (data *OracleDataProvider) Close() error {
	if err := data.sqlxDB.Close(); err != nil {
		return xerrors.Errorf("Can't close DB: %w", err)
	}
	return nil
}

// End of base DataProvider interface

// Begin of base SnapshotProvider interface

func (data *OracleDataProvider) BeginSnapshot() error {
	if !data.config.UseParallelTableLoad {
		return nil
	}

	if data.snaphotSplitTransaction != nil {
		return xerrors.New("Snapshot already started")
	}

	transaction, err := data.sqlxDB.Beginx()
	if err != nil {
		return xerrors.Errorf("Can't create snapshot split transaction: %w", err)
	}

	data.snaphotSplitTransaction = transaction
	return nil
}

func (data *OracleDataProvider) DataObjects(filter abstract2.DataObjectFilter) (abstract2.DataObjects, error) {
	return oracle_snapshot.NewOracleDataObjects(data.databaseSchema, filter), nil
}

func (data *OracleDataProvider) TableSchema(part abstract2.DataObjectPart) (*abstract.TableSchema, error) {
	oracleDataPart, ok := part.(*oracle_snapshot.OracleDataObject)
	if !ok {
		return nil, xerrors.New("Part must be Oracle data part")
	}

	return oracleDataPart.Table().ToOldTable()
}

func (data *OracleDataProvider) DataObjectsToTableParts(filter abstract2.DataObjectFilter) ([]abstract.TableDescription, error) {
	objects, err := data.DataObjects(filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't get data objects: %w", err)
	}

	tableDescriptions, err := abstract2.DataObjectsToTableParts(objects, filter)
	if err != nil {
		return nil, xerrors.Errorf("Can't convert data objects to table descriptions: %w", err)
	}

	return tableDescriptions, nil
}

func (data *OracleDataProvider) CreateSnapshotSource(part abstract2.DataObjectPart) (abstract2.ProgressableEventSource, error) {
	oracleDataPart, ok := part.(*oracle_snapshot.OracleDataObject)
	if !ok {
		return nil, xerrors.New("Part must be Oracle data part")
	}

	var position *oracle_common.LogPosition
	if data.tracker != nil {
		var err error
		position, err = data.tracker.ReadPosition()
		if err != nil {
			return nil, xerrors.Errorf("Can't read current SCN: %w", err)
		}
	} else {
		var err error
		position, err = oracle_common.NewLogPosition(0, nil, nil, oracle_common.PositionSnapshotStarted, time.Now())
		if err != nil {
			return nil, xerrors.Errorf("cannot make current log position: %w", err)
		}
	}

	// Parallel table source
	if data.config.UseParallelTableLoad {
		tableSource, err := oracle_snapshot.NewParallelTableSource(
			data.sqlxDB,
			data.snaphotSplitTransaction,
			&data.config,
			position,
			oracleDataPart.Table(),
			data.logger,
			data.sourceStats,
		)
		if err != nil {
			return nil, xerrors.Errorf("Can't create parallel table source: %w", err)
		}
		return tableSource, nil
	}

	// Default table source
	tableSource, err := oracle_snapshot.NewTableSource(
		data.sqlxDB, &data.config, position, oracleDataPart.Table(), data.logger, data.sourceStats)
	if err != nil {
		return nil, xerrors.Errorf("Can't create table source: %w", err)
	}
	return tableSource, nil
}

func (data *OracleDataProvider) EndSnapshot() error {
	if !data.config.UseParallelTableLoad {
		return nil
	}

	if data.snaphotSplitTransaction == nil {
		return xerrors.New("Snapshot not started")
	}

	if err := data.snaphotSplitTransaction.Rollback(); err != nil {
		return xerrors.Errorf("Can't rollback snapshot split transaction: %w", err)
	}

	return nil
}

func (data *OracleDataProvider) ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (abstract2.DataObjectPart, error) {
	db, err := data.DatabaseSchema()
	if err != nil {
		return nil, xerrors.Errorf("Cannot get database schema: %w", err)
	}

	schema := db.OracleSchemaByName(tableDesc.Schema)
	if schema == nil {
		return nil, xerrors.Errorf("Cannot find schema '%v'", tableDesc.Schema)
	}

	table := schema.OracleTableByName(tableDesc.Name)
	if table == nil {
		return nil, xerrors.Errorf("Cannot find table '%v.%v'", tableDesc.Schema, tableDesc.Name)
	}

	return oracle_snapshot.NewOracleDataObject(table), nil
}

func (data *OracleDataProvider) TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (abstract2.DataObjectPart, error) {
	if tableDescription == nil {
		return nil, nil
	}
	return data.ResolveOldTableDescriptionToDataPart(*tableDescription)
}

// End of base SnapshotProvider interface

// Begin of base ReplicationProvider interface

func (data *OracleDataProvider) CreateReplicationSource() (abstract2.EventSource, error) {
	logMinerSource, err := logminer.NewLogMinerSource(
		data.sqlxDB,
		&data.config,
		data.databaseSchema,
		data.tracker,
		data.logger,
		data.sourceStats,
	)
	if err != nil {
		return nil, xerrors.Errorf("Can't create replication source: %w", err)
	}
	return logMinerSource, nil
}

// End of base ReplicationProvider interface

// Begin of base TrackerProvider interface

func (data *OracleDataProvider) ResetTracker(typ abstract.TransferType) error {
	if data.tracker != nil {
		current, err := data.getCurrentLogPosition(typ)
		if err != nil {
			return xerrors.Errorf("Can't read current SCN: %w", err)
		}
		if err := data.tracker.WritePosition(current); err != nil {
			return xerrors.Errorf("Can't write current SCN to tracker: %w", err)
		}
	}
	return nil
}

// End of base TrackerProvider interface
