package greenplum

import "github.com/transferia/transferia/pkg/abstract/model"

type Connection struct {
	ClusterId        string
	CoordinatorHosts []*Host
	User             string
	Password         model.SecretString
	Databases        []string
	CACertificates   string
}

func (gp *Connection) GetDatabases() []string {
	return gp.Databases
}

func (gp *Connection) GetClusterID() string {
	return gp.ClusterId
}

func (gp *Connection) GetUsername() string {
	return gp.User
}

func (gp *Connection) HostNames() []string {
	names := make([]string, len(gp.CoordinatorHosts))
	for i, host := range gp.CoordinatorHosts {
		names[i] = host.Name
	}
	return names
}

// connection resolver should validate connection before resolving master host
// if no master host is found, we should return first unspecified host and it should be single host in connection
func (gp *Connection) ResolveMasterHost() *Host {
	for _, host := range gp.CoordinatorHosts {
		if host.Role == RoleMaster {
			return host
		}
	}
	for _, host := range gp.CoordinatorHosts {
		if host.Role == RoleUndefined {
			return host
		}
	}
	return nil
}

// return first replica host or nil if no replica host
func (gp *Connection) ResolveReplicaHost() *Host {
	for _, host := range gp.CoordinatorHosts {
		if host.Role == RoleReplica {
			return host
		}
	}
	return nil
}
