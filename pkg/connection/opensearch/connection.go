package opensearch

import (
	"github.com/transferia/transferia/pkg/abstract/model"
)

type Connection struct {
	Hosts          []*Host
	User           string
	Password       model.SecretString
	HasTLS         bool
	CACertificates string
	ClusterID      string
}

func (ch *Connection) GetDatabases() []string {
	return []string{}
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
