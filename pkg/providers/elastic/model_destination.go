//go:build !disable_elastic_provider

package elastic

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type ElasticSearchHostPort struct {
	Host string
	Port int
}

type ElasticSearchDestination struct {
	ClusterID        string // Deprecated: new endpoints should be on premise only
	DataNodes        []ElasticSearchHostPort
	User             string
	Password         model.SecretString
	SSLEnabled       bool
	TLSFile          string
	SubNetworkID     string
	SecurityGroupIDs []string
	Cleanup          model.CleanupType
	ConnectionID     string

	SanitizeDocKeys bool
}

var _ model.Destination = (*ElasticSearchDestination)(nil)

func (d *ElasticSearchDestination) ToElasticSearchDestination() (*ElasticSearchDestination, ServerType) {
	return d, ElasticSearch
}

func (d *ElasticSearchDestination) Hosts() []string {
	result := make([]string, 0)
	for _, el := range d.DataNodes {
		result = append(result, el.Host)
	}
	return result
}

func (d *ElasticSearchDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *ElasticSearchDestination) Validate() error {
	if d.MDBClusterID() == "" &&
		len(d.DataNodes) == 0 {
		return xerrors.Errorf("no host specified")
	}
	if !d.SSLEnabled && len(d.TLSFile) > 0 {
		return xerrors.Errorf("can't use CA certificate with disabled SSL")
	}
	return nil
}

func (d *ElasticSearchDestination) WithDefaults() {
}

func (d *ElasticSearchDestination) VPCSubnets() []string {
	if d.SubNetworkID == "" {
		return nil
	}
	return []string{d.SubNetworkID}
}

func (d *ElasticSearchDestination) VPCSecurityGroups() []string {
	return d.SecurityGroupIDs
}

func (d *ElasticSearchDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *ElasticSearchDestination) IsDestination() {}

func (d *ElasticSearchDestination) Transformer() map[string]string {
	// TODO: this is a legacy method. Drop it when it is dropped from the interface.
	return make(map[string]string)
}

func (d *ElasticSearchDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *ElasticSearchDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	if transferType == abstract.TransferTypeSnapshotOnly || model.IsAppendOnlySource(src) {
		return nil
	}
	return xerrors.Errorf("ElasticSearch target supports only AppendOnly sources or snapshot transfers")
}
