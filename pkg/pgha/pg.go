package pgha

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/postgres/pgerrors"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.yandex/hasql"
	"golang.yandex/hasql/checkers"
)

const (
	defaultPoolSize = 150
)

type PgHA struct {
	user      string
	password  string
	dbName    string
	cluster   *hasql.Cluster
	port      int
	ssl       bool
	pools     map[string]*pgxpool.Pool
	poolMutex sync.Mutex
	poolSize  int64
}

func (pg *PgHA) Close() error {
	var err error
	if pg.cluster != nil {
		err = pg.cluster.Close()
		pg.cluster = nil
	}
	for _, pool := range pg.pools {
		pool.Close()
	}
	return err
}

// hostByRole resolves the address of the appropriate cluster node using the provided context.
// The caller's context is respected so that request deadlines are propagated correctly.
func (pg *PgHA) hostByRole(ctx context.Context, role dbaas.Role) (*string, error) {
	var node hasql.Node
	var err error
	if role == dbaas.ANY {
		node, err = pg.cluster.WaitForPrimaryPreferred(ctx)
	} else if role == dbaas.REPLICA {
		node, err = pg.cluster.WaitForStandby(ctx)
	} else {
		node, err = pg.cluster.WaitForPrimary(ctx)
	}
	if err != nil {
		if role == dbaas.MASTER {
			return nil, coded.Errorf(error_codes.PostgresMasterUnavailable, "There are no available hosts with role %v: %w", role, err)
		}
		return nil, xerrors.Errorf("There are no available hosts with role %v: %w", role, err)
	}

	str := node.Addr()
	return &str, nil
}

func (pg *PgHA) ReplicaHost() (*string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return pg.hostByRole(ctx, dbaas.REPLICA)
}

func (pg *PgHA) MasterHost() (*string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return pg.hostByRole(ctx, dbaas.MASTER)
}

func (pg *PgHA) ConnString(role dbaas.Role) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	host, err := pg.hostByRole(ctx, role)
	if err != nil {
		return "", xerrors.Errorf("Failed to get host with role %v: %w", role, err)
	}
	return pg.ConnStringByHost(*host), nil
}

func (pg *PgHA) ConnStringByHost(host string) string {
	q := fmt.Sprintf(
		`host=%v port=%v dbname=%s user=%s password=%s`,
		host,
		pg.port,
		pg.dbName,
		pg.user,
		pg.password)
	if pg.ssl {
		q = q + " sslmode=verify-full"
	}
	return q
}

func (pg *PgHA) ConnConfig(role dbaas.Role) (*pgx.ConnConfig, error) {
	connString, err := pg.ConnString(role)
	if err != nil {
		return nil, xerrors.Errorf("cannot parse connection string: %w", err)
	}
	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, xerrors.Errorf("cannot parse connection string: %w", err)
	}

	connConfig.PreferSimpleProtocol = true
	return connConfig, nil
}

func (pg *PgHA) PoolExec(ctx context.Context, role dbaas.Role, f func(*pgxpool.Conn) error) error {
	conn, err := pg.Pool(ctx, role)
	if err != nil {
		return xerrors.Errorf("Failed to build pool: %w", err)
	}

	return conn.AcquireFunc(ctx, f)
}

// Pool returns (creating if necessary) a connection pool for the given role.
// ConnString resolution (which includes waiting for a healthy cluster node) is performed
// OUTSIDE the mutex so that one slow hasql wait does not block all other concurrent DB operations.
func (pg *PgHA) Pool(ctx context.Context, role dbaas.Role) (*pgxpool.Pool, error) {
	// Resolve the connection string outside the mutex. hostByRole may block while waiting
	// for hasql to identify the primary; holding the mutex during that wait would serialize
	// all concurrent DB operations across the process.
	connString, err := pg.hostByRole(ctx, role)
	if err != nil {
		return nil, xerrors.Errorf("Failed to resolve host for role %v: %w", role, err)
	}
	fullConnString := pg.ConnStringByHost(*connString)

	pg.poolMutex.Lock()
	defer pg.poolMutex.Unlock()

	if pool, ok := pg.pools[fullConnString]; ok {
		return pool, nil
	}
	dsn, err := pgxpool.ParseConfig(fullConnString)
	if err != nil {
		return nil, xerrors.Errorf("Failed to parse connection configuration: %w", err)
	}
	dsn.ConnConfig.RuntimeParams["standard_conforming_strings"] = "on"
	dsn.ConnConfig.PreferSimpleProtocol = true
	dsn.MinConns = 0
	dsn.MaxConns = int32(pg.poolSize)
	pool, err := pgxpool.ConnectConfig(context.Background(), dsn)
	if err != nil {
		return nil, xerrors.Errorf("Failed to init pool: %w", err)
	}
	pg.pools[fullConnString] = pool

	return pool, nil
}

func (pg *PgHA) Sqlx(role dbaas.Role) (*sqlx.DB, error) {
	conn, err := pg.ConnString(role)
	if err != nil {
		return nil, xerrors.Errorf("Failed to build connection string: %w", err)
	}
	dsn, err := pgx.ParseConfig(conn)
	if err != nil {
		return nil, xerrors.Errorf("Failed to parse connection configuration: %w", err)
	}
	dsn.RuntimeParams["standard_conforming_strings"] = "on"
	dsn.PreferSimpleProtocol = true
	dbx := sqlx.NewDb(stdlib.OpenDB(*dsn), "pgx")
	return dbx, nil
}

func (pg *PgHA) Conn(role dbaas.Role) (*pgx.Conn, error) {
	connConfig, err := pg.ConnConfig(role)
	if err != nil {
		return nil, xerrors.Errorf("cannot create connection config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (pg *PgHA) PoolSize() int64 {
	return pg.poolSize
}

func NewFromConnection(managedConnection *connection.ConnectionPG) (*PgHA, error) {
	hosts := make([]string, 0, len(managedConnection.Hosts))
	for _, host := range managedConnection.Hosts {
		hosts = append(hosts, fmt.Sprintf("%s:%v", host.Name, host.Port))
	}
	return NewFromHosts(managedConnection.Database, managedConnection.User, string(managedConnection.Password),
		hosts, 0, managedConnection.HasTLS)
}

func NewFromHosts(dbName, user, password string, hosts []string, port int, ssl bool) (*PgHA, error) {
	if port == 0 {
		port = 6432 // default port in MDB
	}
	if len(hosts) == 0 {
		return nil, xerrors.New("no host found")
	}
	var nodes []hasql.Node
	var pingErrs []error
	for _, host := range hosts {
		// if port present in 'host' string - it overrides 'port' from config
		realHost, realPort, err := dbaas.ResolveHostPortWithOverride(host, uint16(port))
		if err != nil {
			return nil, xerrors.Errorf("unable to extract host/port, host:%s, port:%d", host, port)
		}
		connStr := fmt.Sprintf(
			`host=%s port=%d dbname=%s user=%s password=%s`,
			realHost,
			realPort,
			dbName,
			user,
			password,
		)
		if ssl {
			connStr = connStr + " sslmode=verify-full"
		}
		dsn, err := pgx.ParseConfig(connStr)
		if err != nil {
			return nil, xerrors.Errorf("Cannot parse connection string: %w", err)
		}
		dsn.PreferSimpleProtocol = true
		db := stdlib.OpenDB(*dsn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := db.PingContext(ctx); err != nil {
			logger.Log.Warnf("Unable to ping host %v, err: %v", host, err)
			pingErrs = append(pingErrs, err)
			cancel()
			if err := db.Close(); err != nil {
				logger.Log.Warn("unable to close db", log.Error(err))
			}
			continue
		}
		cancel()
		nodes = append(nodes, hasql.NewNode(host, db)) // we use here 'host', not 'realHost' intentionally - to be able to get further node port
	}
	if len(pingErrs) > 0 && len(pingErrs) == len(hosts) {
		logger.Log.Error("unable to ping any host", log.Any("error", pingErrs))
		joinedErr := errors.Join(pingErrs...)
		if pgerrors.AnyErrHasCode(pingErrs, pgerrors.ErrcInvalidCatalogName) {
			return nil, coded.Errorf(error_codes.PostgresDatabaseNotFound, "All hosts are unavailable: database does not exist: %w", joinedErr)
		}
		if pgerrors.AnyErrHasCode(pingErrs, pgerrors.ErrcInvalidPassword, pgerrors.ErrcInvalidAuthSpec) {
			return nil, coded.Errorf(error_codes.InvalidCredential, "All hosts are unavailable due to authentication failure: %w", joinedErr)
		}
		return nil, coded.Errorf(error_codes.PostgresAllHostsUnavailable, "All hosts are unavailable: %w", joinedErr)
	}
	// Use options to fine-tune cluster behaviour
	opts := []hasql.ClusterOption{
		hasql.WithUpdateInterval(2 * time.Second), // set custom update interval
	}
	c, err := hasql.NewCluster(nodes, checkers.PostgreSQL, opts...)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create HA cluster: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	_, err = c.WaitForAlive(ctx)
	if err != nil {
		return nil, xerrors.Errorf("Failed to wait for cluster availability: %w", err)
	}
	return &PgHA{
		user:      user,
		password:  password,
		dbName:    dbName,
		cluster:   c,
		port:      port,
		ssl:       ssl,
		pools:     map[string]*pgxpool.Pool{},
		poolMutex: sync.Mutex{},
		poolSize:  defaultPoolSize,
	}, nil
}

func NewFromDBAAS(name, user, password, cluster string) (*PgHA, error) {
	instc, err := dbaas.Current()
	if err != nil {
		return nil, xerrors.Errorf("unable to init dbaas: %w", err)
	}
	resolver, err := instc.HostResolver(dbaas.ProviderTypePostgresql, cluster)
	if err != nil {
		return nil, xerrors.Errorf("unable to create resolver: %w", err)
	}
	apiHosts, err := resolver.ResolveHosts()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve hosts: %w", err)
	}
	logger.Log.Info("cluster resolved to hosts", log.String("cluster", cluster), log.Any("hosts", apiHosts))
	if len(apiHosts) == 0 {
		return nil, xerrors.Errorf("No hosts found for cluster %s", cluster)
	}

	replicaTypePresent := false
	for _, ah := range apiHosts {
		if ah.ReplicaType != "" && ah.ReplicaType != dbaas.ReplicaTypeUnknown {
			replicaTypePresent = true
			break
		}
	}

	hosts := make([]string, 0)
	for _, ah := range apiHosts {
		if replicaTypePresent {
			if ah.Role == dbaas.REPLICA && ah.ReplicaType != dbaas.ReplicaTypeSync {
				continue
			}
		}
		if ah.Health != dbaas.ALIVE {
			continue
		}
		hosts = append(hosts, ah.Name)
	}
	if len(hosts) == 0 {
		return nil, xerrors.Errorf("No compatible hosts found for cluster %s", cluster)
	}

	return NewFromHosts(name, user, password, hosts, 0, true)
}

func NewFromHASQL(cluster *hasql.Cluster, dbName, user, password string, port int, ssl bool) *PgHA {
	return &PgHA{
		user:      user,
		password:  password,
		dbName:    dbName,
		cluster:   cluster,
		port:      port,
		ssl:       ssl,
		pools:     map[string]*pgxpool.Pool{},
		poolMutex: sync.Mutex{},
		poolSize:  defaultPoolSize,
	}
}
