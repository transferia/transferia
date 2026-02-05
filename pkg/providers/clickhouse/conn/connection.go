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
	return clickhouse.OpenDB(opts), nil
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
		ReadTimeout: 5 * time.Minute,
	}, nil
}
