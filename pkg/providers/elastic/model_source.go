package elastic

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

type ElasticSearchSource struct {
	ClusterID            string                  `log:"true"` // Deprecated: new endpoints should be on premise only
	DataNodes            []ElasticSearchHostPort `log:"true"`
	User                 string                  `log:"true"`
	Password             model.SecretString
	SSLEnabled           bool `log:"true"`
	TLSFile              string
	SubNetworkID         string   `log:"true"`
	SecurityGroupIDs     []string `log:"true"`
	DumpIndexWithMapping bool     `log:"true"`
	ConnectionID         string   `log:"true"`
	UserEnabledTls       *bool
}

var _ model.Source = (*ElasticSearchSource)(nil)

func (s *ElasticSearchSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

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
		UserEnabledTls:   s.UserEnabledTls,
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
