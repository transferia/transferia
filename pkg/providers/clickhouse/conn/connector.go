package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

var _ driver.Connector = (*resolvingConnector)(nil)

type resolvingConnector struct {
	config       ConnParams
	resolveHosts func(context.Context) ([]*conn_clickhouse.Host, error)
	driver       driver.Driver
}

func (c *resolvingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ClickHouse connection canceled: %w", err)
	}
	hosts, err := c.resolveHosts(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to refresh ClickHouse hosts: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ClickHouse connection canceled after host resolution: %w", err)
	}
	if len(hosts) == 0 {
		return nil, xerrors.New("no ClickHouse hosts available")
	}
	options, err := GetClickhouseOptions(c.config, hosts)
	if err != nil {
		return nil, xerrors.Errorf("unable to configure ClickHouse connection: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ClickHouse connection canceled after configuration: %w", err)
	}
	connection, err := clickhouse_go.Connector(options).Connect(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to connect to ClickHouse replicas: %w", err)
	}
	return connection, nil
}

func (c *resolvingConnector) Driver() driver.Driver {
	return c.driver
}

// ConnectNativeWithResolver creates a ClickHouse connection pool.
//
// When the pool needs a new connection, it first calls resolveHosts to get the host list.
// Production callers use ConnectionHosts to resolve the current endpoint
// hosts. For MDB, it returns only hosts marked ALIVE by the MDB API; discovery
// does not probe their connectivity.
//
// Hosts can be added or replaced while a transfer is running. Resolving the list
// again lets the pool use those hosts without restarting the transfer.
func ConnectNativeWithResolver(config ConnParams, resolveHosts func(context.Context) ([]*conn_clickhouse.Host, error)) (*sql.DB, error) {
	if resolveHosts == nil {
		return nil, xerrors.New("ClickHouse host resolver is not configured")
	}
	hosts, err := resolveHosts(context.Background())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve ClickHouse hosts: %w", err)
	}
	if len(hosts) == 0 {
		return nil, xerrors.New("no ClickHouse hosts available")
	}
	options, err := GetClickhouseOptions(config, hosts)
	if err != nil {
		return nil, xerrors.Errorf("unable to configure ClickHouse connection: %w", err)
	}
	connector := &resolvingConnector{
		config:       config,
		resolveHosts: resolveHosts,
		driver:       clickhouse_go.Connector(options).Driver(),
	}
	db := sql.OpenDB(connector)
	// Preserve the pool defaults applied by clickhouse_go.OpenDB.
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)
	return db, nil
}
