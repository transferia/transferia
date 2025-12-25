package clickhouse

import (
	"github.com/transferia/transferia/pkg/abstract/model"
)

type Connection struct {
	// TODO: add shard params
	Hosts    []*Host `log:"true"`
	User     string  `log:"true"`
	Password model.SecretString
	// currently filled with user data, not from db list in managed connection
	Database       string `log:"true"`
	HasTLS         bool   `log:"true"`
	CACertificates string
	ClusterID      string `log:"true"`
	// field in manage connection with applicable databases, currently used for info only.
	// in the future we may want to check that DatabaseNames if defined includes Database from user input
	DatabaseNames []string            `log:"true"`
	ShardGroups   map[string][]string `log:"true"`
}

func (ch *Connection) GetDatabases() []string {
	return ch.DatabaseNames
}

func (ch *Connection) GetClusterID() string {
	return ch.ClusterID
}

func (ch *Connection) GetUsername() string {
	return ch.User
}

func (ch *Connection) HostNames() []string {
	names := make([]string, len(ch.Hosts))
	for i, host := range ch.Hosts {
		names[i] = host.Name
	}
	return names
}
