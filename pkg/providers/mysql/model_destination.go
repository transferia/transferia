package mysql

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
)

type TableName = string

type MysqlDestination struct {
	AllowReplace         bool
	Cleanup              model.CleanupType
	ClusterID            string
	Database             string
	DisableParallelWrite map[TableName]bool
	Host                 string
	IsPublic             bool
	MaintainTables       bool
	MaxParralelWriters   int64
	Password             model.SecretString
	PerTransactionPush   bool
	Port                 int
	ProgressTrackerDB    string
	SecurityGroupIDs     []string
	SkipKeyChecks        bool
	SQLMode              string
	SubNetworkID         string
	Timezone             string
	TLSFile              string
	TransformerConfig    map[string]string
	User                 string

	BufferTriggingSize     uint64
	BufferTriggingInterval time.Duration

	RootCAFiles []string

	// Used for snapshot in runtime only
	prevSkipKeyChecks         bool
	prevPerTransactionPush    bool
	ConnectionID              string
	IsSchemaMigrationDisabled bool
}

var (
	_ model.Destination          = (*MysqlDestination)(nil)
	_ model.WithConnectionID     = (*MysqlDestination)(nil)
	_ model.AlterableDestination = (*MysqlDestination)(nil)
)

func (d *MysqlDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *MysqlDestination) IsAlterable() {}

func (d *MysqlDestination) GetConnectionID() string {
	return d.ConnectionID
}

func (d *MysqlDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *MysqlDestination) ReliesOnSystemTablesTransferring() bool {
	return d.PerTransactionPush
}

func (d *MysqlDestination) WithDefaults() {
	if d.MaxParralelWriters <= 0 {
		d.MaxParralelWriters = 4
	}
	if d.Port <= 0 {
		d.Port = 3306
	}
	if d.SQLMode == "" {
		d.SQLMode = "NO_AUTO_VALUE_ON_ZERO" + // если в колонку с автоинкрементом приходит значение 0 или null, то так и вставляем
			",NO_DIR_IN_CREATE" + // игнорируем ручное указание папок для файликов бд
			",NO_ENGINE_SUBSTITUTION" // явно требуем указать движок для таблицы
	}
	if d.Cleanup == "" {
		d.Cleanup = model.Drop
	}
	if d.DisableParallelWrite == nil {
		d.DisableParallelWrite = map[TableName]bool{}
	}
	if d.Timezone == "" {
		d.Timezone = "Local"
	}
	if d.ProgressTrackerDB == "" {
		d.ProgressTrackerDB = d.Database
	}
}

func (d *MysqlDestination) PreSnapshotHacks() {
	// We must skip key checks since order of table upload may be incorrect
	d.prevSkipKeyChecks = d.SkipKeyChecks
	d.SkipKeyChecks = true
	d.prevPerTransactionPush = d.PerTransactionPush
	d.PerTransactionPush = false
}

func (d *MysqlDestination) PostSnapshotHacks() {
	d.SkipKeyChecks = d.prevSkipKeyChecks
	d.PerTransactionPush = d.prevPerTransactionPush
}

func (d *MysqlDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     d.BufferTriggingSize,
		TriggingInterval: d.BufferTriggingInterval,
	}
}

func (d *MysqlDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (d *MysqlDestination) HasTLS() bool {
	return d.ClusterID != "" || d.TLSFile != ""
}

func (MysqlDestination) IsDestination() {
}

func (d *MysqlDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *MysqlDestination) Validate() error {
	return nil
}

func (d *MysqlDestination) ToStorageParams() *MysqlStorageParams {
	return &MysqlStorageParams{
		ClusterID:           d.ClusterID,
		Host:                d.Host,
		Port:                d.Port,
		User:                d.User,
		Password:            string(d.Password),
		Database:            d.Database,
		TLS:                 d.HasTLS(),
		CertPEMFile:         d.TLSFile,
		UseFakePrimaryKey:   false,
		DegreeOfParallelism: 1,
		Timezone:            d.Timezone,
		TableFilter:         nil,
		PreSteps:            DefaultMysqlDumpPreSteps(),
		ConsistentSnapshot:  false,
		RootCAFiles:         d.RootCAFiles,
		ConnectionID:        d.ConnectionID,
	}
}
