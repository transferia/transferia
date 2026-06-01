package conn

import (
	"database/sql"
	"strconv"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

func ConnectNative(host *conn_clickhouse.Host, cfg ConnParams, hosts ...*conn_clickhouse.Host) (*sql.DB, error) {
	opts, err := GetClickhouseOptions(cfg, append([]*conn_clickhouse.Host{host}, hosts...))
	if err != nil {
		return nil, err
	}
	return clickhouse_go.OpenDB(opts), nil
}

func GetClickhouseOptions(cfg ConnParams, hosts []*conn_clickhouse.Host) (*clickhouse_go.Options, error) {
	tlsConfig, err := NewTLS(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to load tls: %w", err)
	}

	addrs := yslices.Map(hosts, func(host *conn_clickhouse.Host) string {
		portStr := strconv.Itoa(host.NativePort)
		return host.Name + ":" + portStr
	})

	password, err := cfg.ResolvePassword()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve password: %w", err)
	}

	return &clickhouse_go.Options{
		TLS:  tlsConfig,
		Addr: addrs,
		Auth: clickhouse_go.Auth{
			Database: cfg.Database(),
			Username: cfg.User(),
			Password: password,
		},
		Compression: &clickhouse_go.Compression{Method: clickhouse_go.CompressionLZ4},
		// Use timeouts from v1 driver to preserve its behaviour.
		// See https://github.com/ClickHouse/clickhouse-go/blob/v1.5.4/bootstrap.go#L23
		DialTimeout: 5 * time.Second,
		ReadTimeout: cfg.ReadTimeout(),
	}, nil
}
