package opensearch

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/elastic"
)

type OpenSearchSource struct {
	ClusterID            string
	DataNodes            []OpenSearchHostPort
	User                 string
	Password             model.SecretString
	SSLEnabled           bool
	TLSFile              string
	SubNetworkID         string
	SecurityGroupIDs     []string
	DumpIndexWithMapping bool
	ConnectionID         string
}

var _ model.Source = (*OpenSearchSource)(nil)
var _ model.WithConnectionID = (*OpenSearchSource)(nil)

func (s *OpenSearchSource) MDBClusterID() string {
	return s.ClusterID
}

func (s *OpenSearchSource) ToElasticSearchSource() (*elastic.ElasticSearchSource, elastic.ServerType) {
	dataNodes := make([]elastic.ElasticSearchHostPort, 0)
	for _, el := range s.DataNodes {
		dataNodes = append(dataNodes, elastic.ElasticSearchHostPort(el))
	}
	return &elastic.ElasticSearchSource{
		ClusterID:            s.ClusterID,
		DataNodes:            dataNodes,
		User:                 s.User,
		Password:             s.Password,
		SSLEnabled:           s.SSLEnabled,
		TLSFile:              s.TLSFile,
		SubNetworkID:         s.SubNetworkID,
		SecurityGroupIDs:     s.SecurityGroupIDs,
		DumpIndexWithMapping: s.DumpIndexWithMapping,
		ConnectionID:         s.ConnectionID,
	}, elastic.OpenSearch
}

func (s *OpenSearchSource) IsSource() {
}

func (s *OpenSearchSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *OpenSearchSource) Validate() error {
	if s.ConnectionID != "" {
		return nil
	}
	if s.ClusterID == "" &&
		len(s.DataNodes) == 0 {
		return xerrors.Errorf("no host specified")
	}
	if !s.SSLEnabled && len(s.TLSFile) > 0 {
		return xerrors.Errorf("can't use CA certificate with disabled SSL")
	}
	return nil
}

func (s *OpenSearchSource) GetConnectionID() string {
	return s.ConnectionID
}

func (s *OpenSearchSource) WithDefaults() {
}

func (s *OpenSearchSource) Hosts() []string {
	result := make([]string, 0)
	for _, el := range s.DataNodes {
		result = append(result, el.Host)
	}
	return result
}
