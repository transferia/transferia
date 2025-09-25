package kafka

import (
	"net"
	"strconv"

	"github.com/transferia/transferia/pkg/abstract/model"
)

type KafkaSaslSecurityMechanism string

const (
	KafkaSaslSecurityMechanism_UNSPECIFIED  KafkaSaslSecurityMechanism = ""
	KafkaSaslSecurityMechanism_PLAIN        KafkaSaslSecurityMechanism = "PLAIN"
	KafkaSaslSecurityMechanism_SCRAM_SHA256 KafkaSaslSecurityMechanism = "SHA-256"
	KafkaSaslSecurityMechanism_SCRAM_SHA512 KafkaSaslSecurityMechanism = "SHA-512"
)

type Connection struct {
	ClusterID string
	Hosts     []*Host
	User      string
	Password  model.SecretString
	// currently filled with user data, not from db list in managed connection
	Database       string
	HasTLS         bool
	CACertificates string
	Mechanisms     []KafkaSaslSecurityMechanism
}

func (c *Connection) GetClusterID() string {
	return c.ClusterID
}

func (c *Connection) GetDatabases() []string {
	return []string{}
}

func (c *Connection) HostNames() []string {
	hosts := make([]string, 0, len(c.Hosts))
	for _, host := range c.Hosts {
		hosts = append(hosts, host.Name)
	}

	return hosts
}

func (c *Connection) GetUsername() string {
	return c.User
}

type Host struct {
	Name string
	Port int
}

func (c *Connection) ToBrokersUrls() []string {
	brokers := make([]string, 0, len(c.Hosts))
	for _, host := range c.Hosts {
		brokers = append(brokers, net.JoinHostPort(host.Name, strconv.Itoa(host.Port)))
	}
	return brokers
}
