package mongo

import (
	"github.com/transferia/transferia/pkg/connection"
)

var _ connection.ManagedConnection = (*Connection)(nil)

type Host struct {
	Name string
	Port int
	Type HostType
	Role HostRole
}

type HostType int

const (
	HostTypeUndefined  HostType = 0
	HostTypeMongod     HostType = 1
	HostTypeMongos     HostType = 2
	HostTypeMongoInfra HostType = 3
)

type HostRole int

const (
	HostRoleUndefined HostRole = 0
	HostRolePrimary   HostRole = 1
	HostRoleSecondary HostRole = 2
)

type Connection struct {
	Hosts          []*Host
	User           string
	Password       string
	HasTLS         bool
	CACertificates string
	ClusterID      string
	DatabaseNames  []string

	// if exist MONGOS/MONGOINFRA hosts then it is sharded cluster
	Sharded bool
}

func (c *Connection) GetClusterID() string {
	return c.ClusterID
}

func (c *Connection) GetDatabases() []string {
	return c.DatabaseNames
}

func (c *Connection) GetUsername() string {
	return c.User
}

func (c *Connection) GetPassword() string {
	return c.Password
}

func (c *Connection) GetPort(hostName string) int {
	for _, host := range c.Hosts {
		if host.Name == hostName {
			return host.Port
		}
	}
	return 0
}

func (c *Connection) HostNames() []string {
	hosts := make([]string, 0, len(c.Hosts))
	for _, host := range c.Hosts {
		hosts = append(hosts, host.Name)
	}
	return hosts
}
