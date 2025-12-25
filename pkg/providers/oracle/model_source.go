package oracle

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.uber.org/zap/zapcore"
)

func init() {
	gobwrapper.RegisterName("*server.OracleSource", new(OracleSource))
	model.RegisterSource(ProviderType, func() model.LoggableSource {
		return new(OracleSource)
	})
	abstract.RegisterProviderName(ProviderType, "Oracle")
}

const ProviderType = abstract.ProviderType("oracle")

type OracleSource struct {
	// Public properties

	// Connection type
	ConnectionType OracleConnectionType `log:"true"`

	// Instance connection
	Host        string `log:"true"`
	Port        int    `log:"true"`
	SID         string `log:"true"`
	ServiceName string `log:"true"`

	// TNS connection string connection
	TNSConnectString string `log:"true"`

	// Credentials
	User     string `log:"true"`
	Password model.SecretString

	// Other settings
	SubNetworkID     string   `log:"true"` // Yandex Cloud VPC Network
	SecurityGroupIDs []string `log:"true"` // Yandex Cloud VPC Security Groups
	PDB              string   `log:"true"` // Used in CDB environment

	// Tables filter
	IncludeTables []string `log:"true"`
	ExcludeTables []string `log:"true"`

	ConvertNumberToInt64 bool `log:"true"`

	// Hidden properties
	TrackerType                          OracleLogTrackerType      `log:"true"`
	CLOBReadingStrategy                  OracleCLOBReadingStrategy `log:"true"`
	UseUniqueIndexesAsKeys               bool                      `log:"true"`
	IsNonConsistentSnapshot              bool                      `log:"true"` // Do not use flashback
	UseParallelTableLoad                 bool                      `log:"true"` // Split tables to parts by ROWID
	ParallelTableLoadDegreeOfParallelism int                       `log:"true"` // Works with UseParallelTableLoad, how many readers for table
}

var _ model.Source = (*OracleSource)(nil)

type OracleConnectionType string

const (
	OracleSIDConnection         = OracleConnectionType("SID")
	OracleServiceNameConnection = OracleConnectionType("ServiceName")
	OracleTNSConnection         = OracleConnectionType("TNS")
)

type OracleLogTrackerType string

const (
	OracleNoLogTracker       = OracleLogTrackerType("NoLogTracker")
	OracleInMemoryLogTracker = OracleLogTrackerType("InMemory")
	OracleEmbeddedLogTracker = OracleLogTrackerType("Embedded")
	OracleInternalLogTracker = OracleLogTrackerType("Internal")
)

type OracleCLOBReadingStrategy string

const (
	OracleReadCLOB                       = OracleCLOBReadingStrategy("ReadCLOB")
	OracleReadCLOBAsBLOB                 = OracleCLOBReadingStrategy("ReadCLOBAsBLOB")
	OracleReadCLOBAsBLOBIfFunctionExists = OracleCLOBReadingStrategy("ReadCLOBAsBLOBIfFunctionExists")
)

func (oracle *OracleSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(oracle, enc)
}

func (oracle *OracleSource) CDB() bool {
	return oracle.PDB != ""
}

func (oracle *OracleSource) WithDefaults() {
	if oracle.Port == 0 {
		oracle.Port = 1521
	}
	if oracle.ConnectionType == "" {
		oracle.ConnectionType = OracleTNSConnection
	}
	if oracle.TrackerType == "" {
		oracle.TrackerType = OracleInternalLogTracker
	}
	if oracle.CLOBReadingStrategy == "" {
		oracle.CLOBReadingStrategy = OracleReadCLOBAsBLOBIfFunctionExists
	}
	if oracle.ParallelTableLoadDegreeOfParallelism == 0 {
		oracle.ParallelTableLoadDegreeOfParallelism = 4
	}
}

func (OracleSource) IsSource() {}

func (oracle *OracleSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (oracle *OracleSource) Validate() error {
	return nil
}

func (oracle *OracleSource) IsAbstract2(model.Destination) bool {
	return true
}

func (oracle *OracleSource) SupportMultiWorkers() bool {
	return false
}

func (oracle *OracleSource) SupportMultiThreads() bool {
	return true
}
