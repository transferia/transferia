package opensearch

import (
	"github.com/transferia/transferia/pkg/abstract/model"
)

type Connection struct {
	Hosts          []*Host `log:"true"`
	User           string  `log:"true"`
	Password       model.SecretString
	HasTLS         bool `log:"true"`
	CACertificates string
	ClusterID      string `log:"true"`
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
