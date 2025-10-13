package oracle

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

func init() {
	gobwrapper.RegisterName("*server.OracleSource", new(OracleSource))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(OracleSource)
	})
	abstract.RegisterProviderName(ProviderType, "Oracle")
}

const ProviderType = abstract.ProviderType("oracle")

type OracleSource struct {
	// Public properties

	// Connection type
	ConnectionType OracleConnectionType

	// Instance connection
	Host        string
	Port        int
	SID         string
	ServiceName string

	// TNS connection string connection
	TNSConnectString string

	// Credentials
	User     string
	Password model.SecretString

	// Other settings
	SubNetworkID     string   // Yandex Cloud VPC Network
	SecurityGroupIDs []string // Yandex Cloud VPC Security Groups
	PDB              string   // Used in CDB environment

	// Tables filter
	IncludeTables []string
	ExcludeTables []string

	ConvertNumberToInt64 bool

	// Hidden properties
	TrackerType                          OracleLogTrackerType
	CLOBReadingStrategy                  OracleCLOBReadingStrategy
	UseUniqueIndexesAsKeys               bool
	IsNonConsistentSnapshot              bool // Do not use flashback
	UseParallelTableLoad                 bool // Split tables to parts by ROWID
	ParallelTableLoadDegreeOfParallelism int  // Works with UseParallelTableLoad, how many readers for table
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
