//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/pgha"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const SelectCurrentLsnDelay = `select pg_wal_lsn_diff(pg_last_wal_replay_lsn(), $1);`

// replica specific stuff
func getHostPreferablyReplica(lgr log.Logger, conn *connection.ConnectionPG, slotID string) (*connection.Host, error) {
	if conn.ClusterID == "" {
		replHost, err := getReplicaOnPrem(conn)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve replica for on-prem: %w", err)
		}
		return replHost, nil
	}
	return getReplicaManaged(lgr, conn, slotID)
}

func getReplicaManaged(lgr log.Logger, conn *connection.ConnectionPG, slotID string) (*connection.Host, error) {
	master, aliveAsyncReplicas, aliveSyncReplicas, err := detectHostsByRoles(lgr, conn)
	if err != nil {
		return nil, xerrors.Errorf("Failed to resolve cluster hosts roles:  %w", err)
	}

	if len(aliveAsyncReplicas) == 0 && len(aliveSyncReplicas) == 0 {
		lgr.Warn("No alive replicas found, will use master host instead")
		return master, nil
	}

	// in case we need replica for making a snapshot for Copy-and-replicate (CDC) transfer,
	// we need to make sure that replica host is up-to-date and has LSN not less than master's replication slot LSN
	// if no up-to-date replica was found we should keep using master host
	lsn, err := resolveSlotLSN(slotID, toConnParams(master, conn))
	if err != nil {
		return nil, xerrors.Errorf("Failed to resolve lsn from master due to some error:  %w", err)
	}
	if lsn == "" {
		lgr.Info("Do not need to check replica's LSN before using - no cdc replication slot was found")
		return append(aliveAsyncReplicas, aliveSyncReplicas...)[0], nil
	}

	lgr.Info("Will check replica's LSN before using")
	// using async replica if available
	if len(aliveAsyncReplicas) > 0 {
		logger.Log.Info("Will check async replicas state first")
		host, err := findNonStaleReplica(conn, lsn, aliveAsyncReplicas)
		if err != nil {
			lgr.Info("Error finding async replica host", log.Error(err))
		}
		if host != nil {
			return host, nil
		}
	}

	if len(aliveSyncReplicas) > 0 {
		lgr.Info("Checking sync replicas state")
		return findNonStaleReplica(conn, lsn, aliveSyncReplicas)
	}

	return nil, xerrors.Errorf("Could not find replica with Lsn greater than %v", lsn)
}

func getReplicaOnPrem(conn *connection.ConnectionPG) (*connection.Host, error) {
	pg, err := pgha.NewFromConnection(conn)
	if err != nil {
		return nil, xerrors.Errorf("unable to create postgres service client for resolving replica from hosts: %w", err)
	}
	defer pg.Close()

	// If there is a master, then it is better to choose him. If there is no master among
	// the hosts, then we take any replica, because lsn cannot be checked anyway.
	_, err = pg.MasterHost()
	if err != nil && !xerrors.Is(err, context.DeadlineExceeded) {
		return nil, xerrors.Errorf("unable to check master in cluster before resolving replica: %w", err)
	}
	if err == nil {
		return nil, xerrors.New("there is master, unable to check replica's lsn for on-prem")
	}

	replicaNode, err := pg.ReplicaHost()
	if err != nil {
		return nil, xerrors.Errorf("unable to get replica host: %w", err)
	}
	if replicaNode == nil {
		return nil, xerrors.Errorf("ReplicaHost() returned nil")
	}

	replHost, replPort, err := getHostPortFromMDBHostname(*replicaNode, conn)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse replica host, err: %w", err)
	}

	return &connection.Host{
		Name:        replHost,
		Port:        int(replPort),
		Role:        connection.Replica,
		ReplicaType: connection.ReplicaUndefined,
	}, nil
}

func detectHostsByRoles(lgr log.Logger, conn *connection.ConnectionPG) (master *connection.Host, aliveAsyncReplicas []*connection.Host, aliveSyncReplicas []*connection.Host, err error) {
	aliveAsyncReplicas = make([]*connection.Host, 0)
	aliveSyncReplicas = make([]*connection.Host, 0)

	if master = conn.MasterHost(); master != nil {
		// roles are already detected in connection - this is managed connection for managed pg cluster
		// however replica type is not always available - seems it is filled only when async is present
		for _, host := range conn.Hosts {
			if host.Role != connection.Replica {
				continue
			}
			if host.ReplicaType == connection.ReplicaAsync {
				aliveAsyncReplicas = append(aliveAsyncReplicas, host)
			} else {
				lgr.Infof("Found replica host %s with type %s", host.Name, host.ReplicaType)
				aliveSyncReplicas = append(aliveSyncReplicas, host)
			}
		}
	} else {
		// roles are unknown - this is manual connection or on-premise pg installation
		var clusterHosts []dbaas.ClusterHost
		clusterHosts, err = dbaas.ResolveClusterHosts(dbaas.ProviderTypePostgresql, conn.ClusterID)
		lgr.Info("Resolved cluster to hosts via dbaas", log.String("cluster", conn.ClusterID), log.Any("hosts", clusterHosts))
		if err != nil {
			return nil, nil, nil, xerrors.Errorf("unable to resolve postgres hosts: %w", err)
		}
		for _, clusterHost := range clusterHosts {
			hostName, port, err := getHostPortFromMDBHostname(clusterHost.Name, conn)
			if err != nil {
				return nil, nil, nil, xerrors.Errorf("unable to resolve port: %w", err)
			}
			host := connection.SimpleHost(hostName, int(port))
			if clusterHost.Role == dbaas.MASTER {
				master = host
			} else if clusterHost.Health == dbaas.ALIVE {
				if clusterHost.ReplicaType == dbaas.ReplicaTypeAsync {
					aliveAsyncReplicas = append(aliveAsyncReplicas, host)
				} else {
					aliveSyncReplicas = append(aliveSyncReplicas, host)
				}
			}
		}
	}
	return master, aliveAsyncReplicas, aliveSyncReplicas, nil
}

func getHostPortFromMDBHostname(hostname string, connPG *connection.ConnectionPG) (string, uint16, error) {
	// get from name if available, otherwise getting from connection param (user input)
	portFromConfig := connPG.GetPort(hostname)
	if portFromConfig == 0 {
		logger.Log.Info("No port present in config, using default mdb port")
		portFromConfig = 6432
	}
	resolvedHost, resolvedPort, err := dbaas.ResolveHostPortWithOverride(hostname, portFromConfig)
	if err != nil {
		return "", 0, err
	}
	logger.Log.Infof("Resolved mdb host %s to host: %s, port: %v", hostname, resolvedHost, resolvedPort)
	return resolvedHost, resolvedPort, nil
}

// Resolve slot LSN on master for current transfer_id
// see https://www.postgresql.org/docs/current/view-pg-replication-slots.html
func resolveSlotLSN(slotID string, connParams *ConnectionParams) (string, error) {
	var lsn string

	conn, err := makeConnPoolFromParams(connParams, logger.Log)
	if err != nil {
		logger.Log.Warn("Failed to make connection to master", log.Error(err))
		return "", xerrors.Errorf("Failed to make connection to master: %w", err)
	}
	defer conn.Close()

	if err = conn.QueryRow(context.TODO(), SelectLsnForSlot, slotID).Scan(&lsn); err != nil {
		if err == pgx.ErrNoRows {
			logger.Log.Info("No replication slot found for transfer, will use any replica", log.Error(err))
			return "", nil
		}
		logger.Log.Warn("Failed to make get lsn to check replica status", log.Error(err))
		return "", xerrors.Errorf("Failed to get master lsn to check replica status, will use master instead: %w", err)
	}

	logger.Log.Infof("Master slot lsn is %v", lsn)
	return lsn, nil
}

// Choose replica host with LSN after master's slot LSN in order to make sure that no data will be lost
// between making snapshot and getting increments from WAL
// see pg_last_wal_replay_lsn https://www.postgresql.org/docs/15/functions-admin.html
func findNonStaleReplica(conn *connection.ConnectionPG, lsn string, aliveReplicas []*connection.Host) (*connection.Host, error) {
	var replicaHost *connection.Host
	err := backoff.Retry(func() error {
		for _, replica := range aliveReplicas {
			if isReplicaUpToDate(toConnParams(replica, conn), lsn) {
				replicaHost = replica
				return nil
			}
		}
		logger.Log.Infof("Could not find replica with Lsn greater than %v between hosts %v", lsn, aliveReplicas)
		return xerrors.Errorf("Could not find replica with Lsn greater than %v", lsn)
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5))
	if err != nil {
		return nil, err
	}
	logger.Log.Info("Found the fresh replicaHost!", log.String("replicaHost", replicaHost.Name))
	return replicaHost, nil
}

func isReplicaUpToDate(connParams *ConnectionParams, lsn string) bool {
	conn, err := makeConnPoolFromParams(connParams, logger.Log)
	if err != nil {
		logger.Log.Warn("Failed to make connection to replica", log.Error(err))
		return false
	}
	defer conn.Close()

	var replicaLsnDiff int

	if err = conn.QueryRow(context.TODO(), SelectCurrentLsnDelay, lsn).Scan(&replicaLsnDiff); err != nil {
		logger.Log.Warn("Failed to get replica wal lsn, will use master instead", log.Error(err))
		return false
	}

	logger.Log.Info("Got lsn diff for replica", log.String("replicaHost", connParams.Host), log.Int("diff", replicaLsnDiff))
	return replicaLsnDiff >= 0
}

func getMasterConnectionParams(lgr log.Logger, connParams *connection.ConnectionPG) (*ConnectionParams, error) {
	// master was resolved in connection
	if connParams.MasterHost() != nil {
		return toConnParams(connParams.MasterHost(), connParams), nil
	}
	// find suitable host manually
	masterHost, masterPort, err := resolveMasterHostImpl(connParams)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host, err: %w", err)
	}
	lgr.Infof("postgres master host/port: %s:%d", masterHost, masterPort)
	return &ConnectionParams{
		Host:           masterHost,
		Port:           masterPort,
		Database:       connParams.Database,
		User:           connParams.User,
		Password:       connParams.Password,
		HasTLS:         connParams.HasTLS,
		CACertificates: connParams.CACertificates,
		ClusterID:      connParams.ClusterID,
	}, nil
}

func resolveMasterHostImpl(conn *connection.ConnectionPG) (string, uint16, error) {
	if master := conn.MasterHost(); master != nil {
		// it is resolved connection for cluster
		return master.Name, uint16(master.Port), nil
	}

	if len(conn.Hosts) == 1 {
		return conn.Hosts[0].Name, uint16(conn.Hosts[0].Port), nil
	}

	var pg *pgha.PgHA
	var err error

	if len(conn.Hosts) != 0 {
		// it is on-prem installation - via plain conf or managed connection
		pg, err = pgha.NewFromConnection(conn)
		if err != nil {
			return "", 0, xerrors.Errorf("unable to create postgres service client from hosts: %w", err)
		}
		defer pg.Close()
	} else {
		// it is mdb cluster NOT from managed connection, so hosts still need to be resolved
		pg, err = pgha.NewFromDBAAS(conn.Database, conn.User, string(conn.Password), conn.ClusterID)
		if err != nil {
			return "", 0, xerrors.Errorf("unable to create postgres service client from clusterID: %w", err)
		}
		defer pg.Close()
	}

	masterNode, err := pg.MasterHost()
	if err != nil {
		return "", 0, xerrors.Errorf("unable to get master host: %w", err)
	}
	if masterNode == nil {
		return "", 0, xerrors.New("MasterHost() returned nil")
	}

	resultHost, resultPort, err := getHostPortFromMDBHostname(*masterNode, conn)
	if err != nil {
		return "", 0, xerrors.Errorf("unable to parse master host, err: %w", err)
	}
	return resultHost, resultPort, nil
}

func makeConnConfigFromParams(connParams *ConnectionParams) (*pgx.ConnConfig, error) {
	return makeConnConfig(connParams, "", false)
}

func makeConnConfig(connParams *ConnectionParams, connString string, tryHostCAcertificates bool) (*pgx.ConnConfig, error) {
	tlsConfig, err := getTLSConfig(connParams.Host, connParams.HasTLS, connParams.CACertificates, connParams.ClusterID, tryHostCAcertificates)
	if err != nil {
		return nil, err
	}
	config, _ := pgx.ParseConfig(connString)
	config.Host = connParams.Host
	config.Port = connParams.Port
	config.Database = connParams.Database
	config.User = connParams.User
	config.Password = string(connParams.Password)
	config.TLSConfig = tlsConfig
	config.PreferSimpleProtocol = true
	return config, nil
}

func getTLSConfig(host string, hasTLS bool, tlsFile string, cluster string, tryHostCAcertificates bool) (*tls.Config, error) {
	// what changes for pg source/dst (but not storage!!) : use custom cert for mdb if present
	if hasTLS {
		rootCertPool := x509.NewCertPool()
		if len(tlsFile) > 0 {
			if ok := rootCertPool.AppendCertsFromPEM([]byte(tlsFile)); !ok {
				return nil, xerrors.New("unable to add TLS to cert pool")
			}
		}
		return &tls.Config{
			RootCAs:    rootCertPool,
			ServerName: host,

			InsecureSkipVerify: len(tlsFile) == 0,
		}, nil
	}

	if cluster != "" || tryHostCAcertificates {
		return &tls.Config{ServerName: host}, nil
	}

	logger.Log.Warn("insecure connection is used", log.String("pg_host", host))
	return nil, nil
}

func GetConnParamsFromSrc(lgr log.Logger, src *PgSource) (*ConnectionParams, error) {
	var conn *connection.ConnectionPG
	var err error
	if src.ConnectionID != "" {
		conn, err = resolveConnection(src.ConnectionID, src.Database)
	} else {
		conn = makeConnectionFromSrc(src)
	}

	if err != nil {
		return nil, xerrors.Errorf("Could not resolve connection %w", err)
	}

	return getMasterConnectionParams(lgr, conn)
}

func MakeConnConfigFromSrc(lgr log.Logger, pgSrc *PgSource) (*pgx.ConnConfig, error) {
	connParams, err := GetConnParamsFromSrc(lgr, pgSrc)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host: %w", err)
	}
	return makeConnConfigFromParams(connParams)
}

func MakeConnConfigFromSink(lgr log.Logger, params PgSinkParams) (*pgx.ConnConfig, error) {
	var conn *connection.ConnectionPG
	var err error
	if params.ConnectionID() != "" {
		conn, err = resolveConnection(params.ConnectionID(), params.Database())
	} else {
		conn = makeConnectionFromSink(params)
	}

	if err != nil {
		return nil, xerrors.Errorf("Could not resolve connection %w", err)
	}

	connParams, err := getMasterConnectionParams(lgr, conn)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host: %w", err)
	}
	return makeConnConfigFromParams(connParams)
}

func MakeConnConfigFromStorage(lgr log.Logger, storage *PgStorageParams) (*pgx.ConnConfig, error) {
	var conn *connection.ConnectionPG
	var err error
	if storage.ConnectionID != "" {
		conn, err = resolveConnection(storage.ConnectionID, storage.Database)
	} else {
		conn = makeConnectionFromStorage(storage)
	}

	if err != nil {
		return nil, xerrors.Errorf("Could not resolve connection %w", err)
	}

	var connParams *ConnectionParams
	// When 'PreferReplica' is false OR 'ResolveReplicaHost' didn't find any replica - we try to get master:
	// - if on_prem one host - master is this host
	// - if on_prem >1 hosts - pgHA determines master
	// - if managed installation - master is determined via mdb api
	if storage.PreferReplica {
		if host, err := getHostPreferablyReplica(lgr, conn, storage.SlotID); err != nil {
			lgr.Warn("unable to resolve replica host, will try to resolve master", log.Error(err))
		} else {
			connParams = toConnParams(host, conn)
		}
	}

	if connParams == nil {
		connParams, err = getMasterConnectionParams(lgr, conn)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve master host: %w", err)
		}
	}

	return makeConnConfig(connParams, storage.ConnString, storage.TryHostCACertificates)
}

// MakeConnPoolFrom*

func MakeConnPoolFromSrc(pgSrc *PgSource, lgr log.Logger) (*pgxpool.Pool, error) {
	connParams, err := GetConnParamsFromSrc(lgr, pgSrc)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host from source: %w", err)
	}
	return makeConnPoolFromParams(connParams, lgr)
}

func MakeConnPoolFromDst(pgDst *PgDestination, lgr log.Logger) (*pgxpool.Pool, error) {
	var conn *connection.ConnectionPG
	var err error
	if pgDst.ConnectionID != "" {
		conn, err = resolveConnection(pgDst.ConnectionID, pgDst.Database)
	} else {
		conn = makeConnectionFromDst(pgDst)
	}

	if err != nil {
		return nil, xerrors.Errorf("Could not resolve connection %w", err)
	}

	connParams, err := getMasterConnectionParams(lgr, conn)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host from destination: %w", err)
	}
	return makeConnPoolFromParams(connParams, lgr)
}

func makeConnPoolFromParams(connParams *ConnectionParams, lgr log.Logger) (*pgxpool.Pool, error) {
	logger.Log.Info("Using pg host to establish connection", log.String("pg_host", connParams.Host), log.UInt16("pg_port", connParams.Port))
	connConfig, err := makeConnConfigFromParams(connParams)
	if err != nil {
		return nil, xerrors.Errorf("unable to make connection config: %w", err)
	}
	return NewPgConnPool(connConfig, lgr)
}

func NewPgConnPool(connConfig *pgx.ConnConfig, lgr log.Logger, dataTypesOptions ...DataTypesOption) (*pgxpool.Pool, error) {
	connConfig.PreferSimpleProtocol = true

	connConfigCopy := connConfig
	if lgr == nil {
		connConfigCopy.Logger = nil
	} else {
		connConfigCopy = WithLogger(connConfig, log.With(lgr, log.Any("component", "pgx")))
	}

	poolConfig, _ := pgxpool.ParseConfig("")
	poolConfig.ConnConfig = connConfigCopy
	poolConfig.MaxConns = 5
	poolConfig.AfterConnect = MakeInitDataTypes(dataTypesOptions...)

	pool, err := NewPgConnPoolConfig(context.TODO(), poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to make a connection pool: %w", err)
	}
	return pool, nil
}

// pgStatementTimeout returns the GUC `statement_timeout` or a default timeout if the former is zero
func pgStatementTimeout(ctx context.Context, conn *pgx.Conn) (time.Duration, error) {
	sql := "select setting from pg_settings where name = 'statement_timeout'"
	var rawResult string
	if err := conn.QueryRow(ctx, sql).Scan(&rawResult); err != nil {
		return 0, xerrors.Errorf("failed to execute SQL '%v': %w", sql, err)
	}
	rawResultParsed, err := strconv.ParseInt(rawResult, 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("failed to parse statement_timeout: %w", err)
	}
	result := time.Duration(rawResultParsed)

	if result <= 0 {
		return 5 * time.Minute, nil
	} else {
		return result * time.Millisecond, nil
	}
}

// NewPgConnPoolConfig creates a connection pool. It provides built-in timeouts to limit the duration of connection attempts
func NewPgConnPoolConfig(ctx context.Context, poolConfig *pgxpool.Config) (*pgxpool.Pool, error) {
	basicCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	pgxConn, err := pgx.ConnectConfig(basicCtx, poolConfig.ConnConfig)
	if err != nil {
		if IsPgError(err, ErrcInvalidPassword) || IsPgError(err, ErrcInvalidAuthSpec) {
			return nil, coded.Errorf(providers.InvalidCredential, "failed to connect to a PostgreSQL instance: %w", err)
		}
		return nil, xerrors.Errorf("failed to connect to a PostgreSQL instance: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := pgxConn.Close(closeCtx); err != nil {
			logger.Log.Error("Failed to close connection used to obtain timeout", log.Error(err))
		}
	}()
	statementTimeout, err := pgStatementTimeout(basicCtx, pgxConn)
	if err != nil {
		return nil, xerrors.Errorf("failed to get statement timeout from a PostgreSQL instance: %w", err)
	}
	cancel()

	goodTimeoutCtx, cancel := context.WithTimeout(ctx, 5*statementTimeout)
	defer cancel()
	result, err := pgxpool.ConnectConfig(goodTimeoutCtx, poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to a PostgreSQL instance or create a connection pool: %w", err)
	}

	return result, nil
}

type ConnectionParams struct {
	Host           string
	Port           uint16
	Database       string
	User           string
	Password       model.SecretString
	HasTLS         bool
	CACertificates string
	ClusterID      string
}

func toConnParams(host *connection.Host, params *connection.ConnectionPG) *ConnectionParams {
	return &ConnectionParams{
		Host:           host.Name,
		Port:           uint16(host.Port),
		Database:       params.Database,
		User:           params.User,
		Password:       params.Password,
		HasTLS:         params.HasTLS,
		CACertificates: params.CACertificates,
		ClusterID:      params.ClusterID,
	}
}

func makeConnectionFromSrc(src *PgSource) *connection.ConnectionPG {
	connParams := &connection.ConnectionPG{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{},
			User:           src.User,
			Password:       src.Password,
			ClusterID:      src.ClusterID,
			Database:       src.Database,
			HasTLS:         src.HasTLS(),
			CACertificates: src.TLSFile,
		},
		DatabaseNames: []string{src.Database},
	}
	connParams.SetHosts(src.AllHosts(), src.Port)
	return connParams
}

func makeConnectionFromDst(dst *PgDestination) *connection.ConnectionPG {
	connParams := &connection.ConnectionPG{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{},
			User:           dst.User,
			Password:       dst.Password,
			ClusterID:      dst.ClusterID,
			Database:       dst.Database,
			HasTLS:         dst.HasTLS(),
			CACertificates: dst.TLSFile,
		},
		DatabaseNames: []string{dst.Database},
	}
	connParams.SetHosts(dst.AllHosts(), dst.Port)
	return connParams
}

func makeConnectionFromStorage(dst *PgStorageParams) *connection.ConnectionPG {
	connParams := &connection.ConnectionPG{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{},
			User:           dst.User,
			Password:       model.SecretString(dst.Password),
			ClusterID:      dst.ClusterID,
			Database:       dst.Database,
			HasTLS:         dst.HasTLS(),
			CACertificates: dst.TLSFile,
		},
		DatabaseNames: []string{dst.Database},
	}
	connParams.SetHosts(dst.AllHosts, dst.Port)
	return connParams
}

func makeConnectionFromSink(dst PgSinkParams) *connection.ConnectionPG {
	connParams := &connection.ConnectionPG{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{},
			User:           dst.User(),
			Password:       model.SecretString(dst.Password()),
			ClusterID:      dst.ClusterID(),
			Database:       dst.Database(),
			HasTLS:         dst.HasTLS(),
			CACertificates: dst.TLSFile(),
		},
		DatabaseNames: []string{dst.Database()},
	}
	connParams.SetHosts(dst.AllHosts(), dst.Port())
	return connParams
}

func resolveConnection(connectionID string, database string) (*connection.ConnectionPG, error) {
	connCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// DP agent token here
	conn, err := connection.Resolver().ResolveConnection(connCtx, connectionID, ProviderType)
	if err != nil {
		return nil, err
	}

	if pgConn, ok := conn.(*connection.ConnectionPG); ok {
		pgConn.Database = database
		return pgConn, nil
	}

	return nil, xerrors.Errorf("Cannot cast connection %s to PG connection", connectionID)
}
