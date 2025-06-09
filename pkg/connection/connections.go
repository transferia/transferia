package connection

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/connection/opensearch"
)

var _ ManagedConnection = (*ConnectionPG)(nil)
var _ ManagedConnection = (*ConnectionMySQL)(nil)
var _ ManagedConnection = (*clickhouse.Connection)(nil)
var _ ManagedConnection = (*opensearch.Connection)(nil)

type ConnectionPG struct {
	*BaseSQLConnection
	// field in manage connection with applicable databases, currently used for info only.
	// in the future we may want to check that DatabaseNames if defined includes Database from user input
	DatabaseNames []string
}

func (pg *ConnectionPG) GetDatabases() []string {
	return pg.DatabaseNames
}

type ConnectionMySQL struct {
	*BaseSQLConnection
	// field in manage connection with applicable databases, currently used for info only.
	// in the future we may want to check that DatabaseNames if defined includes Database from user input
	DatabaseNames []string
}

func (m *ConnectionMySQL) GetDatabases() []string {
	return m.DatabaseNames
}

type BaseSQLConnection struct {
	Hosts    []*Host
	User     string
	Password model.SecretString
	// currently filled with user data, not from db list in managed connection
	Database       string
	HasTLS         bool
	CACertificates string
	ClusterID      string
}

func (b *BaseSQLConnection) GetUsername() string {
	return b.User
}

func (b *BaseSQLConnection) GetClusterID() string {
	return b.ClusterID
}

func (b *BaseSQLConnection) MasterHost() *Host {
	for _, host := range b.Hosts {
		if host.Role == Master {
			return host
		}
	}
	return nil
}

func (b *BaseSQLConnection) Replicas() []*Host {
	hosts := make([]*Host, 0)
	for _, host := range b.Hosts {
		if host.Role == Replica {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

func (b *BaseSQLConnection) SetHosts(hostNames []string, port int) {
	hosts := make([]*Host, 0, len(hostNames))
	for _, host := range hostNames {
		hosts = append(hosts, SimpleHost(host, port))
	}
	b.Hosts = hosts
}

func (b *BaseSQLConnection) GetPort(hostName string) uint16 {
	for _, host := range b.Hosts {
		if host.Name == hostName {
			return uint16(host.Port)
		}
	}
	return 0
}

func (b *BaseSQLConnection) HostNames() []string {
	hosts := make([]string, 0, len(b.Hosts))
	for _, host := range b.Hosts {
		hosts = append(hosts, host.Name)
	}
	return hosts
}

type Host struct {
	Name        string
	Port        int
	Role        Role
	ReplicaType ReplicaType
}

func SimpleHost(host string, port int) *Host {
	return &Host{
		Name:        host,
		Port:        port,
		Role:        RoleUnknown,
		ReplicaType: ReplicaUndefined,
	}
}

type Role string

const (
	Master      = Role("MASTER")
	Replica     = Role("REPLICA")
	RoleUnknown = Role("REPLICA")
)

type ReplicaType string

const (
	ReplicaSync      = ReplicaType("SYNC")
	ReplicaAsync     = ReplicaType("ASYNC")
	ReplicaUndefined = ReplicaType("UNDEFINED")
)
