package greenplum

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract/model"
)

type PgSinkParamsRegulated struct {
	FClusterID                string
	FAllHosts                 []string
	FPort                     int
	FDatabase                 string
	FUser                     string
	FPassword                 string
	FTLSFile                  string
	FMaintainTables           bool
	FPerTransactionPush       bool
	FLoozeMode                bool
	IsSchemaMigrationDisabled bool
	FCleanupMode              model.CleanupType
	FTables                   map[string]string
	FCopyUpload               bool
	FIgnoreUniqueConstraint   bool
	FDisableSQLFallback       bool
	FQueryTimeout             time.Duration
}

func (p PgSinkParamsRegulated) GetIsSchemaMigrationDisabled() bool {
	return p.IsSchemaMigrationDisabled
}

func (p PgSinkParamsRegulated) ClusterID() string {
	return p.FClusterID
}

func (p PgSinkParamsRegulated) AllHosts() []string {
	return p.FAllHosts
}

func (p PgSinkParamsRegulated) Port() int {
	return p.FPort
}

func (p PgSinkParamsRegulated) Database() string {
	return p.FDatabase
}

func (p PgSinkParamsRegulated) User() string {
	return p.FUser
}

func (p PgSinkParamsRegulated) Password() string {
	return string(p.FPassword)
}

func (p PgSinkParamsRegulated) HasTLS() bool {
	return len(p.TLSFile()) > 0
}

func (p PgSinkParamsRegulated) TLSFile() string {
	return p.FTLSFile
}

func (p PgSinkParamsRegulated) MaintainTables() bool {
	return p.FMaintainTables
}

func (p PgSinkParamsRegulated) PerTransactionPush() bool {
	return p.FPerTransactionPush
}

func (p PgSinkParamsRegulated) LoozeMode() bool {
	return p.FLoozeMode
}

func (p PgSinkParamsRegulated) CleanupMode() model.CleanupType {
	return p.FCleanupMode
}

func (p PgSinkParamsRegulated) Tables() map[string]string {
	return p.FTables
}

func (p PgSinkParamsRegulated) CopyUpload() bool {
	return p.FCopyUpload
}

func (p PgSinkParamsRegulated) IgnoreUniqueConstraint() bool {
	return p.FIgnoreUniqueConstraint
}

func (p PgSinkParamsRegulated) DisableSQLFallback() bool {
	return p.FDisableSQLFallback
}

func (p PgSinkParamsRegulated) QueryTimeout() time.Duration {
	return p.FQueryTimeout
}

func (p PgSinkParamsRegulated) ConnectionID() string {
	return ""
}

func GpDestinationToPgSinkParamsRegulated(d *GpDestination) *PgSinkParamsRegulated {
	result := new(PgSinkParamsRegulated)
	result.FDatabase = d.Connection.Database
	result.FUser = d.Connection.User
	result.FPassword = string(d.Connection.AuthProps.Password)
	result.FTLSFile = d.Connection.AuthProps.CACertificate
	result.FMaintainTables = true
	result.IsSchemaMigrationDisabled = true
	result.FCleanupMode = d.CleanupPolicy
	result.FQueryTimeout = d.QueryTimeout
	return result
}
