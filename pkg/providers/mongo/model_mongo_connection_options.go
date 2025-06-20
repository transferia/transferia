//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection"
	mongoConn "github.com/transferia/transferia/pkg/connection/mongo"
)

type HostWithPort struct {
	Host string
	Port int
}

type MongoConnectionOptions struct {
	ClusterID     string
	HostsWithPort []HostWithPort
	ReplicaSet    string
	AuthSource    string
	User          string
	Password      string
	CACert        TrustedCACertificate
	Direct        bool
	SRVMode       bool
	ConnectionID  string
}

// IsDocDB check if we connect to amazon doc DB
func (o *MongoConnectionOptions) IsDocDB() bool {
	for _, h := range o.HostsWithPort {
		if strings.Contains(h.Host, "docdb.amazonaws.com") {
			return true
		}
	}
	return false
}

func (o *MongoConnectionOptions) ResolveCredsByConnectionID(ctx context.Context) error {
	if o.ConnectionID == "" {
		return nil
	}
	conn, err := connection.Resolver().ResolveConnection(ctx, o.ConnectionID, ProviderType)
	if err != nil {
		return err
	}
	mongoConn, ok := conn.(*mongoConn.Connection)
	if !ok {
		return xerrors.Errorf("cannot cast connection %s to Mongo connection", o.ConnectionID)
	}
	o.ClusterID = mongoConn.ClusterID
	o.User = mongoConn.User
	o.Password = mongoConn.Password
	if mongoConn.HasTLS {
		o.CACert = InlineCACertificatePEM([]byte(mongoConn.CACertificates))
	}
	for _, host := range mongoConn.Hosts {
		o.HostsWithPort = append(o.HostsWithPort, HostWithPort{
			Host: host.Name,
			Port: host.Port,
		})
	}
	if mongoConn.ClusterID != "" && !mongoConn.Sharded {
		o.ReplicaSet = "rs01"
	}

	return nil
}

type TrustedCACertificate interface {
	isTrustedCACertificate()
}

type (
	InlineCACertificatePEM    []byte
	CACertificatePEMFilePaths []string
)

func (InlineCACertificatePEM) isTrustedCACertificate()    {}
func (CACertificatePEMFilePaths) isTrustedCACertificate() {}
