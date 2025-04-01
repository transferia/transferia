package greenplum

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

type GpDestination struct {
	Connection GpConnection

	CleanupPolicy dp_model.CleanupType

	SubnetID         string
	SecurityGroupIDs []string

	BufferTriggingSize     uint64
	BufferTriggingInterval time.Duration

	QueryTimeout  time.Duration
	GpfdistParams gpfdistbin.GpfdistParams
}

var _ dp_model.Destination = (*GpDestination)(nil)

func (d *GpDestination) MDBClusterID() string {
	if d.Connection.MDBCluster != nil {
		return d.Connection.MDBCluster.ClusterID
	}
	return ""
}

func (d *GpDestination) IsDestination() {}

func (d *GpDestination) WithDefaults() {
	d.Connection.WithDefaults()
	d.GpfdistParams.WithDefaults()

	if d.CleanupPolicy.IsValid() != nil {
		d.CleanupPolicy = dp_model.DisabledCleanup
	}

	if d.BufferTriggingSize == 0 {
		d.BufferTriggingSize = model.BufferTriggingSizeDefault
	}

	if d.QueryTimeout == 0 {
		d.QueryTimeout = postgres.PGDefaultQueryTimeout
	}
}

func (d *GpDestination) BuffererConfig() *bufferer.BuffererConfig {
	if d.GpfdistParams.IsEnabled {
		// Since gpfdist is only supported for Greenplum source with gpfdist
		// enabled, there is no need in custom bufferer at all.
		return nil
	}
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     d.BufferTriggingSize,
		TriggingInterval: d.BufferTriggingInterval,
	}
}

func (d *GpDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *GpDestination) Validate() error {
	if err := d.Connection.Validate(); err != nil {
		return xerrors.Errorf("invalid connection parameters: %w", err)
	}
	if err := d.CleanupPolicy.IsValid(); err != nil {
		return xerrors.Errorf("invalid cleanup policy: %w", err)
	}
	return nil
}

func (d *GpDestination) Transformer() map[string]string {
	// this is a legacy method. Drop it when it is dropped from the interface.
	return make(map[string]string)
}

func (d *GpDestination) CleanupMode() dp_model.CleanupType {
	return d.CleanupPolicy
}

func (d *GpDestination) ToGpSource() *GpSource {
	return &GpSource{
		Connection:    d.Connection,
		IncludeTables: []string{},
		ExcludeTables: []string{},
		AdvancedProps: *(func() *GpSourceAdvancedProps {
			result := new(GpSourceAdvancedProps)
			result.WithDefaults()
			return result
		}()),
		SubnetID:         "",
		SecurityGroupIDs: nil,
		GpfdistParams:    d.GpfdistParams,
	}
}
