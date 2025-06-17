//go:build !disable_postgres_provider

package postgres

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type PgStorageParams struct {
	AllHosts                    []string // should be non-empty only one field: Hosts/ClusterID
	Port                        int
	User                        string
	Password                    string
	Database                    string
	ClusterID                   string // should be non-empty only one field: Hosts/ClusterID
	TLSFile                     string
	EnableTLS                   bool
	UseFakePrimaryKey           bool
	DBFilter                    []string
	IgnoreUserTypes             bool
	PreferReplica               bool // has the meaning only if ClusterID not empty. Choosing replica is implemented via mdb api. If set as 'true' - it expects you initialized dbaas.Initialize*Cloud
	ExcludeDescendants          bool
	DesiredTableSize            uint64
	SnapshotDegreeOfParallelism int
	ConnString                  string // used in greenplum
	TableFilter                 abstract.Includeable
	TryHostCACertificates       bool // will force SSL connection with host-provided SSL certificates
	UseBinarySerialization      bool // Whether binary serialization format should be used. Defaults to true in homogeneous pg->pg transfers.
	SlotID                      string
	ShardingKeyFields           map[string][]string
	ConnectionID                string
}

// TLSConfigTemplate returns a TLS configuration template without ServerName set.
// It returns nil when a no-TLS connection is requested by the configuration.
func (p *PgStorageParams) TLSConfigTemplate() (*tls.Config, error) {
	result := new(tls.Config)

	if len(p.TLSFile) > 0 {
		// use TLS configuration with user-defined certificates
		rootCertPool := x509.NewCertPool()
		if ok := rootCertPool.AppendCertsFromPEM([]byte(p.TLSFile)); !ok {
			return nil, xerrors.New("unable to add TLS to cert pool")
		}
		result.RootCAs = rootCertPool
		return result, nil
	}

	if len(p.ClusterID) > 0 || p.TryHostCACertificates {
		// use default TLS configuration with host-provided certificates
		return result, nil
	}

	// use no TLS configuration
	return nil, nil
}

// tlsStatusString returns a string describing the TLS connection status of the params
func (p *PgStorageParams) secureConnectionStatusString() string {
	if len(p.TLSFile) > 0 {
		return "secure, user-provided CA certificate"
	}
	if len(p.ClusterID) > 0 {
		return "secure, MDB certificate"
	}
	if p.TryHostCACertificates {
		return "secure, MDB certificate (forced)"
	}
	return "INSECURE connection"
}

func (p *PgStorageParams) HasTLS() bool {
	return p.TLSFile != "" || p.EnableTLS
}

func (p *PgStorageParams) String() string {
	return fmt.Sprintf(
		"%v:%d [%s] (db=`%s`, user=`%s`, connstring=`%s`)",
		p.AllHosts,
		p.Port,
		p.secureConnectionStatusString(),
		p.Database,
		p.User,
		p.ConnString,
	)
}
