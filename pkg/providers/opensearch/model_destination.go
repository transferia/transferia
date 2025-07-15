package opensearch

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/elastic"
)

type OpenSearchHostPort struct {
	Host string
	Port int
}

type OpenSearchDestination struct {
	ClusterID        string
	DataNodes        []OpenSearchHostPort
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

var _ model.Destination = (*OpenSearchDestination)(nil)
var _ model.WithConnectionID = (*OpenSearchDestination)(nil)

func (d *OpenSearchDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *OpenSearchDestination) ToElasticSearchDestination() (*elastic.ElasticSearchDestination, elastic.ServerType) {
	dataNodes := make([]elastic.ElasticSearchHostPort, 0)
	for _, el := range d.DataNodes {
		dataNodes = append(dataNodes, elastic.ElasticSearchHostPort(el))
	}
	return &elastic.ElasticSearchDestination{
		ClusterID:        d.ClusterID,
		DataNodes:        dataNodes,
		User:             d.User,
		Password:         d.Password,
		SSLEnabled:       d.SSLEnabled,
		TLSFile:          d.TLSFile,
		SubNetworkID:     d.SubNetworkID,
		SecurityGroupIDs: d.SecurityGroupIDs,
		Cleanup:          d.Cleanup,
		SanitizeDocKeys:  d.SanitizeDocKeys,
		ConnectionID:     d.ConnectionID,
	}, elastic.OpenSearch
}

func (d *OpenSearchDestination) Hosts() []string {
	result := make([]string, 0)
	for _, el := range d.DataNodes {
		result = append(result, el.Host)
	}
	return result
}

func (d *OpenSearchDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *OpenSearchDestination) Validate() error {
	if d.ConnectionID != "" {
		return nil
	}
	if d.ClusterID == "" &&
		len(d.DataNodes) == 0 {
		return xerrors.Errorf("no host specified")
	}
	if !d.SSLEnabled && len(d.TLSFile) > 0 {
		return xerrors.Errorf("can't use CA certificate with disabled SSL")
	}
	return nil
}

func (d *OpenSearchDestination) GetConnectionID() string {
	return d.ConnectionID
}

func (d *OpenSearchDestination) WithDefaults() {
}

func (d *OpenSearchDestination) IsDestination() {}

func (d *OpenSearchDestination) Transformer() map[string]string {
	// TODO: this is a legacy method. Drop it when it is dropped from the interface.
	return make(map[string]string)
}

func (d *OpenSearchDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *OpenSearchDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	if transferType == abstract.TransferTypeSnapshotOnly || model.IsAppendOnlySource(src) {
		return nil
	}
	return xerrors.Errorf("OpenSearch target supports only AppendOnly sources or snapshot transfers")
}
