package conn

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

func ResolveShards(config model.ChSinkParams, transfer *dp_model.Transfer) error {
	if config.MdbClusterID() != "" {
		shards, err := model.ShardFromCluster(config.MdbClusterID(), config.ChClusterName())
		if err != nil {
			return xerrors.Errorf("failed to obtain a list of shards from MDB ClickHouse: %w", err)
		}

		if len(shards) == 0 {
			return abstract.NewFatalError(xerrors.Errorf("can't find shards for managed ClickHouse '%v'", config.MdbClusterID()))
		}

		config.SetShards(shards)
	} else {
		if len(config.Shards()) == 0 {
			return abstract.NewFatalError(xerrors.New("shards not set for an on-premises ClickHouse"))
		}
	}
	return nil
}

func ConnectNative(host string, cfg ConnParams, hosts ...string) (*sql.DB, error) {
	opts, err := GetClickhouseOptions(cfg, append([]string{host}, hosts...))
	if err != nil {
		return nil, err
	}
	return clickhouse.OpenDB(opts), nil
}

func GetClickhouseOptions(cfg ConnParams, hosts []string) (*clickhouse.Options, error) {
	tlsConfig, err := NewTLS(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to load tls: %w", err)
	}

	portStr := strconv.Itoa(cfg.NativePort())
	addrs := yslices.Map(hosts, func(host string) string {
		return host + ":" + portStr
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
