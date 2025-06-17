//go:build !disable_postgres_provider

package postgres

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract/model"
)

type PgSinkParams interface {
	ClusterID() string
	AllHosts() []string
	Port() int
	Database() string
	User() string
	Password() string
	HasTLS() bool
	TLSFile() string

	// MaintainTables
	// If true - on every batchInsert calls 'create schema...' + 'create table ...'
	// For now, it's not true on every running-transfer
	// It's for lb->pg delivery. Can be auto-derived
	// It's like legacy
	// private option
	MaintainTables() bool
	GetIsSchemaMigrationDisabled() bool
	// PerTransactionPush
	// It's 'SaveTxBoundaries' from proto-spec
	PerTransactionPush() bool
	// LoozeMode
	// If 'true' - when error occurs, we are logging error, and return nil. So it's 'lose data if error'
	// private option
	LoozeMode() bool
	CleanupMode() model.CleanupType
	// Tables
	// It's altnames source->destination
	// private option
	Tables() map[string]string
	// CopyUpload
	// use mechanism 'CopyUpload' for inserts
	CopyUpload() bool
	// IgnoreUniqueConstraint
	// Ignore 'uniqueViolation' error - just go further
	// private option
	IgnoreUniqueConstraint() bool
	// QueryTimeout returns the timeout for query execution by this sink
	QueryTimeout() time.Duration
	// DisableSQLFallback returns true if the sink should never use SQL when copying snapshot and should always use "COPY FROM"
	DisableSQLFallback() bool
	ConnectionID() string
}
