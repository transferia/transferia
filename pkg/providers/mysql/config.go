//go:build !disable_mysql_provider

package mysql

import (
	"crypto/tls"
	"time"
)

// TODO: remove
type Config struct {
	Addr     string
	User     string
	Password string

	Charset         string
	ServerID        uint32
	Flavor          string
	HeartbeatPeriod time.Duration
	ReadTimeout     time.Duration

	// discard row event without table meta
	DiscardNoMetaRowEvent bool

	UseDecimal    bool
	FailOnDecimal bool
	ParseTime     bool

	TimestampStringLocation *time.Location

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool

	// Set to change the maximum number of attempts to re-establish a broken
	// connection
	MaxReconnectAttempts int
	TLSConfig            *tls.Config
	Include              func(db string, table string) bool
}
