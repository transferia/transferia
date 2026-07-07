package init

import (
	"context"
	"strings"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_copy_source "github.com/transferia/transferia/pkg/providers/yt/copy/source"
	"github.com/transferia/transferia/pkg/providers/yt/copy/target"
	_ "github.com/transferia/transferia/pkg/providers/yt/fallback"
	"github.com/transferia/transferia/pkg/providers/yt/lfstaging"
	abstract2_provider_yt "github.com/transferia/transferia/pkg/providers/yt/provider"
	yt_sink "github.com/transferia/transferia/pkg/providers/yt/sink"
	yt_sink_v2 "github.com/transferia/transferia/pkg/providers/yt/sink/v2"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/pkg/targets"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func init() {
	providers.Register(provider_yt.ProviderType, New(provider_yt.ProviderType))
	providers.Register(provider_yt.ManagedProviderType, New(provider_yt.ManagedProviderType))
	providers.Register(provider_yt.ManagedDynamicProviderType, New(provider_yt.ManagedDynamicProviderType))
	providers.Register(provider_yt.ManagedStaticProviderType, New(provider_yt.ManagedStaticProviderType))
	providers.Register(provider_yt.StagingType, New(provider_yt.StagingType))
	providers.Register(provider_yt.CopyType, New(provider_yt.CopyType))
}

// To verify providers contract implementation
var (
	_ providers.Snapshot          = (*Provider)(nil)
	_ providers.Sinker            = (*Provider)(nil)
	_ providers.Abstract2Provider = (*Provider)(nil)
	_ providers.Abstract2Sinker   = (*Provider)(nil)

	_ providers.DstCleanuper = (*Provider)(nil)
	_ providers.Verifier     = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
	provider abstract.ProviderType
}

func (p *Provider) Target(...abstract.SinkOption) (abstract2.EventTarget, error) {
	dst, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination)
	if !ok {
		return nil, targets.UnknownTargetError
	}
	return target.NewTarget(p.logger, p.registry, dst, p.transfer.ID)
}

func (p *Provider) Verify(ctx context.Context) error {
	dst, ok := p.transfer.Dst.(provider_yt.YtDestinationModel)
	if !ok {
		return nil
	}
	if dst.Static() && !p.transfer.SnapshotOnly() {
		return xerrors.New("static yt available only for snapshot copy")
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(provider_yt.YtSourceModel)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return yt_storage.NewStorage(&provider_yt.YtStorageParams{
		Token:                 src.GetYtToken(),
		Cluster:               src.GetCluster(),
		Path:                  src.GetPaths()[0], // TODO: Handle multi-path in abstract 1 yt storage
		Spec:                  nil,
		DisableProxyDiscovery: src.DisableProxyDiscovery(),
		ConnParams:            src,
	})
}

func (p *Provider) DataProvider() (provider abstract2.DataProvider, err error) {
	specificConfig, ok := p.transfer.Src.(provider_yt.YtSourceModel)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	if _, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination); ok {
		provider, err = yt_copy_source.NewSource(p.logger, p.registry, specificConfig, p.transfer.ID)
	} else {
		var include []string
		if p.transfer.DataObjects != nil {
			include = p.transfer.DataObjects.IncludeObjects
		}
		provider, err = abstract2_provider_yt.NewSource(p.logger, p.registry, specificConfig, include)
	}
	return provider, err
}

func (p *Provider) SnapshotSink(config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(provider_yt.YtDestinationModel)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	var s abstract.Sinker
	var err error
	if dst.Static() {
		if !p.transfer.SnapshotOnly() {
			return nil, xerrors.Errorf("failed to create YT (static) sinker: can't make '%s' transfer while sinker is static", p.transfer.Type)
		}

		if dst.Rotation() != nil {
			if s, err = yt_sink.NewRotatedStaticSink(dst, p.registry, p.logger, p.cp, p.transfer.ID); err != nil {
				return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
			}
		} else {
			if s, err = yt_sink_v2.NewStaticSink(dst, p.cp, p.transfer.ID, p.registry, p.logger); err != nil {
				return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
			}
		}
		return s, nil
	}

	if !dst.UseStaticTableOnSnapshot() {
		return p.Sink(config)
	}

	if s, err = yt_sink_v2.NewStaticSinkWrapper(dst, p.cp, p.transfer.ID, p.registry, p.logger); err != nil {
		return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
	}
	return s, nil
}

func (p *Provider) Type() abstract.ProviderType {
	return p.provider
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	if p.provider == provider_yt.StagingType {
		dst, ok := p.transfer.Dst.(*provider_yt.LfStagingDestination)
		if !ok {
			return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
		}
		s, err := lfstaging.NewSinker(dst, getJobIndex(p.transfer), p.transfer, p.logger)
		if err != nil {
			return nil, xerrors.Errorf("failed to create lf staging sinker: %s", err)
		}
		return s, nil
	}
	dst, ok := p.transfer.Dst.(provider_yt.YtDestinationModel)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}

	s, err := yt_sink.NewSinker(dst, p.transfer.ID, p.logger, p.registry)
	if err != nil {
		return nil, xerrors.Errorf("failed to create YT (non-static) sinker: %w", err)
	}
	return s, nil
}

func getJobIndex(transfer *model.Transfer) int {
	if shardingTaskRuntime, ok := transfer.Runtime.(abstract.ShardingTaskRuntime); ok {
		return shardingTaskRuntime.CurrentJobIndex()
	} else {
		return 0
	}
}

func (p *Provider) CleanupSuitable(transferType abstract.TransferType) bool {
	return transferType != abstract.TransferTypeSnapshotOnly
}

func (p *Provider) CleanupDestination(ctx context.Context) error {
	dst, ok := p.transfer.Dst.(provider_yt.YtDestinationModel)
	if !ok {
		return xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}

	// In that case we don't need to cleanup anything, transaction will be aborted
	if dst.Static() || dst.UseStaticTableOnSnapshot() {
		return nil
	}

	if dst.CleanupMode() != model.Replace {
		return nil
	}

	tmpSuffix := model.MakeTmpSuffix(p.transfer.ID, model.TmpTableSuffix)
	client, err := yt_client.FromConnParams(dst, p.logger)
	if err != nil {
		return xerrors.Errorf("error getting YT Client: %w", err)
	}

	if err := provider_yt.HandleNodes(ctx, client, ypath.Path(dst.Path()), nil,
		func(ctx context.Context, client yt.Client, tablePath ypath.Path, attrs *provider_yt.NodeAttrs) error {
			if attrs.Type != yt.NodeTable {
				return nil
			}

			if !strings.HasSuffix(tablePath.String(), tmpSuffix) {
				return nil
			}

			if err := provider_yt.MountUnmountWrapper(
				ctx,
				client,
				tablePath,
				migrate.UnmountAndWait,
			); err != nil {
				p.logger.Error("unable to unmount table", log.Any("path", tablePath), log.Error(err))
				return xerrors.Errorf("unable to unmount table %s : %w", tablePath.String(), err)
			}

			removeOptions := &yt.RemoveNodeOptions{
				Recursive: false,
				Force:     true,
			}
			if err := client.RemoveNode(
				ctx,
				tablePath,
				removeOptions,
			); err != nil {
				return xerrors.Errorf("unable to remove node %s : %w", tablePath.String(), err)
			}
			return nil
		}); err != nil {
		return xerrors.Errorf("unable to cleanup yt path: %w", err)
	}
	return nil
}

func New(provider abstract.ProviderType) func(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return func(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
			provider: provider,
		}
	}
}
