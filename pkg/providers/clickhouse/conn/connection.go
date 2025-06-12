package conn

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
)

func ConnectNative(host *chconn.Host, cfg ConnParams, hosts ...*chconn.Host) (*sql.DB, error) {
	opts, err := GetClickhouseOptions(cfg, append([]*chconn.Host{host}, hosts...))
	if err != nil {
		return nil, err
	}

	db := clickhouse.OpenDB(opts)

	// OpenDB suggests it's the caller's responsibility to configure the connection pool, so we must set it to reasonable defaults for long-running jobs.
	// FIXME: make these configurable
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

func GetClickhouseOptions(cfg ConnParams, hosts []*chconn.Host) (*clickhouse.Options, error) {
	tlsConfig, err := NewTLS(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to load tls: %w", err)
	}

	addrs := yslices.Map(hosts, func(host *chconn.Host) string {
		portStr := strconv.Itoa(host.NativePort)
		return host.Name + ":" + portStr
	})

	password, err := cfg.ResolvePassword()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve password: %w", err)
	}

	return &clickhouse.Options{
		TLS:  tlsConfig,
		Addr: addrs,
		Auth: clickhouse.Auth{
			Database: cfg.Database(),
			Username: cfg.User(),
			Password: password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		// Use timeouts from v1 driver to preserve its behaviour.
		// See https://github.com/ClickHouse/clickhouse-go/blob/v1.5.4/bootstrap.go#L23
		DialTimeout: 5 * time.Second,
		ReadTimeout: time.Minute,
	}, nil
}
