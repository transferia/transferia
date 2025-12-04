package greenplum

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	ch_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"go.uber.org/zap/zapcore"
)

type GpDestination struct {
	Connection GpConnection `log:"true"`

	CleanupPolicy dp_model.CleanupType `log:"true"`

	SubnetID         string   `log:"true"`
	SecurityGroupIDs []string `log:"true"`

	BufferTriggingSize     uint64        `log:"true"`
	BufferTriggingInterval time.Duration `log:"true"`

	QueryTimeout time.Duration `log:"true"`

	EnableGpfdist bool `log:"true"` // EnableGpfdist could be set by FillDependentFields based on the source settings.
}

var _ dp_model.Destination = (*GpDestination)(nil)
var _ dp_model.WithConnectionID = (*GpDestination)(nil)
var _ dp_model.LegacyFillDependentFields = (*GpDestination)(nil)

func (d *GpDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *GpDestination) GetConnectionID() string {
	return d.Connection.ConnectionID
}

func (d *GpDestination) MDBClusterID() string {
	if d.Connection.MDBCluster != nil {
		return d.Connection.MDBCluster.ClusterID
	}
	return ""
}

func (d *GpDestination) IsDestination() {}

func (d *GpDestination) WithDefaults() {
	d.Connection.WithDefaults()

	if d.CleanupPolicy.IsValid() != nil {
		d.CleanupPolicy = dp_model.DisabledCleanup
	}

	if d.BufferTriggingSize == 0 {
		d.BufferTriggingSize = ch_model.BufferTriggingSizeDefault
	}

	if d.QueryTimeout == 0 {
		d.QueryTimeout = postgres.PGDefaultQueryTimeout
	}
}

func (d *GpDestination) BuffererConfig() *bufferer.BuffererConfig {
	if d.EnableGpfdist {
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
			result.DisableGpfdist = !d.EnableGpfdist
			return result
		}()),
		SubnetID:         "",
		SecurityGroupIDs: nil,
	}
}

func (d *GpDestination) FillDependentFields(transfer *dp_model.Transfer) {
	if src, isHomo := transfer.Src.(*GpSource); isHomo {
		d.EnableGpfdist = !src.AdvancedProps.DisableGpfdist
	} else {
		d.EnableGpfdist = false
	}
}
