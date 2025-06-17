//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/dbaas"
)

type ConnectionParams struct {
	Host        string
	Port        int
	User        string
	Password    string
	Database    string
	TLS         bool
	CertPEMFile string
	Location    *time.Location
	RootCAFiles []string
	ClusterID   string
}

func NewConnectionParams(config *MysqlStorageParams) (*ConnectionParams, error) {
	var conn *connection.ConnectionMySQL
	var err error
	if config.ConnectionID != "" {
		conn, err = resolveConnection(config.ConnectionID, config.Database)
	} else {
		conn, err = makeConnectionFromStorage(config)
	}
	if err != nil {
		return nil, err
	}

	var host *connection.Host

	if master := conn.MasterHost(); master != nil {
		// for managed cluster we always know
		host = master
	} else {
		// hosts are never empty here as they are checked above while resolving
		// for on-prem installation we normally have just 1 host in config
		host = conn.Hosts[0]
	}

	params := ConnectionParams{
		Host:        host.Name,
		Port:        host.Port,
		User:        conn.User,
		Password:    string(conn.Password),
		Database:    conn.Database,
		TLS:         conn.HasTLS,
		CertPEMFile: conn.CACertificates,
		Location:    nil,
		RootCAFiles: config.RootCAFiles,
		ClusterID:   conn.ClusterID,
	}
	if err := params.ResolveLocation(config.Timezone); err != nil {
		return nil, xerrors.Errorf("Can't resolve location from value '%v': %w", config.Timezone, err)
	}
	return &params, nil
}

func makeConnectionFromStorage(config *MysqlStorageParams) (*connection.ConnectionMySQL, error) {
	hosts, err := ResolveHosts(config)
	if err != nil {
		return nil, err
	}
	connParams := &connection.ConnectionMySQL{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          hosts,
			User:           config.User,
			Password:       model.SecretString(config.Password),
			ClusterID:      config.ClusterID,
			Database:       config.Database,
			HasTLS:         config.TLS,
			CACertificates: config.CertPEMFile,
		},
		DatabaseNames: []string{config.Database},
	}

	return connParams, nil
}

func ResolveHosts(config *MysqlStorageParams) ([]*connection.Host, error) {
	if config.Host != "" {
		// we have one host - suppose it's master
		return []*connection.Host{{Name: config.Host, Port: config.Port, Role: connection.Master, ReplicaType: connection.ReplicaUndefined}}, nil
	}

	if config.ClusterID == "" {
		return nil, xerrors.New("Neither cluster nor hosts are provided in endpoint config!")
	}

	clusterHosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeMysql, config.ClusterID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get mysql hosts: %w", err)
	}

	var hosts []*connection.Host
	port := config.Port
	if port <= 0 {
		// this should be already set via provider adapter, but just in case
		port = 3306
	}
	for _, host := range clusterHosts {
		hosts = append(hosts, &connection.Host{
			Name:        host.Name,
			Port:        port,
			Role:        getRole(host.Role),
			ReplicaType: connection.ReplicaUndefined,
		})
	}

	if len(hosts) == 0 {
		return nil, xerrors.Errorf("Unable to get mdb hosts via mdb for cluster: %s", config.ClusterID)
	}
	return hosts, nil
}

func getRole(role dbaas.Role) connection.Role {
	if role == dbaas.MASTER {
		return connection.Master
	}
	return connection.Replica
}

func (params *ConnectionParams) ResolveLocation(locationStr string) error {
	location, err := time.LoadLocation(locationStr)
	if err != nil {
		return err
	}
	params.Location = location
	return nil
}

func resolveConnection(connectionID string, database string) (*connection.ConnectionMySQL, error) {
	connCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// DP agent token here
	conn, err := connection.Resolver().ResolveConnection(connCtx, connectionID, ProviderType)
	if err != nil {
		return nil, err
	}

	if mysqlConn, ok := conn.(*connection.ConnectionMySQL); ok {
		mysqlConn.Database = database
		return mysqlConn, nil
	}

	return nil, xerrors.Errorf("Cannot cast connection %s to Mysql connection", connectionID)
}
