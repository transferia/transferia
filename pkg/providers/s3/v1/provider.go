package s3v1

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	s3_v1_sink "github.com/transferia/transferia/pkg/providers/s3/v1/sink"
	"github.com/transferia/transferia/pkg/providers/s3/v1/sink/queue_to_s3_sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(s3_v1_model.ProviderType, New)
}

var (
	_ providers.Sinker        = (*Provider)(nil)
	_ providers.QueueToS3Sink = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer

	operation *model.TransferOperation
}

func (p *Provider) Type() abstract.ProviderType {
	return s3_v1_model.ProviderType
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*s3_v1_model.S3Destination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}

	switch p.transfer.Type {
	case abstract.TransferTypeSnapshotOnly:
		if p.operation == nil {
			return nil, xerrors.Errorf("operation is nil")
		}
		sink, err := s3_v1_sink.NewSnapshotSink(p.logger, dst, p.registry, p.cp, p.transfer.ID, p.operation.CreatedAt.Unix())
		if err != nil {
			return nil, xerrors.Errorf("failed to create snapshot sink: %w", err)
		}
		return sink, nil
	default:
		return nil, xerrors.Errorf("unsupported transfer type: %v", p.transfer.Type)
	}
}

func (p *Provider) AsyncV2Sink(middlewares.Config) (abstract.QueueToS3Sink, error) {
	dst, ok := p.transfer.Dst.(*s3_v1_model.S3Destination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}

	sink, err := queue_to_s3_sink.NewReplicationAsyncSink(p.logger, dst, p.registry)
	if err != nil {
		return nil, xerrors.Errorf("failed to create replication asynchronous sink: %w", err)
	}
	return sink, nil
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, operation *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,

		operation: operation,
	}
}
