//go:build !disable_elastic_provider

package elastic

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type ElasticSearchSource struct {
	ClusterID            string // Deprecated: new endpoints should be on premise only
	DataNodes            []ElasticSearchHostPort
	User                 string
	Password             model.SecretString
	SSLEnabled           bool
	TLSFile              string
	SubNetworkID         string
	SecurityGroupIDs     []string
	DumpIndexWithMapping bool
	ConnectionID         string
}

var _ model.Source = (*ElasticSearchSource)(nil)

func (s *ElasticSearchSource) ToElasticSearchSource() (*ElasticSearchSource, ServerType) {
	return s, ElasticSearch
}

func (s *ElasticSearchSource) SourceToElasticSearchDestination() *ElasticSearchDestination {
	return &ElasticSearchDestination{
		ClusterID:        s.ClusterID,
		DataNodes:        s.DataNodes,
		User:             s.User,
		Password:         s.Password,
		SSLEnabled:       s.SSLEnabled,
		TLSFile:          s.TLSFile,
		SubNetworkID:     s.SubNetworkID,
		SecurityGroupIDs: s.SecurityGroupIDs,
		Cleanup:          "",
		SanitizeDocKeys:  false,
		ConnectionID:     s.ConnectionID,
	}
}

func (s *ElasticSearchSource) IsSource() {
}

func (s *ElasticSearchSource) MDBClusterID() string {
	return s.ClusterID
}

func (s *ElasticSearchSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *ElasticSearchSource) VPCSecurityGroups() []string {
	return s.SecurityGroupIDs
}

func (s *ElasticSearchSource) VPCSubnets() []string {
	if s.SubNetworkID == "" {
		return nil
	}
	return []string{s.SubNetworkID}
}

func (s *ElasticSearchSource) Validate() error {
	if s.MDBClusterID() == "" &&
		len(s.DataNodes) == 0 {
		return xerrors.Errorf("no host specified")
	}
	if !s.SSLEnabled && len(s.TLSFile) > 0 {
		return xerrors.Errorf("can't use CA certificate with disabled SSL")
	}
	return nil
}

func (s *ElasticSearchSource) WithDefaults() {
}
