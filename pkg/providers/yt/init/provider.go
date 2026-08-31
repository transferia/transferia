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
	yt_provider "github.com/transferia/transferia/pkg/providers/yt/provider"
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
	_ providers.SnapshotSinker    = (*Provider)(nil)
	_ providers.Abstract2Provider = (*Provider)(nil)
	_ providers.Abstract2Sinker   = (*Provider)(nil)

	_ providers.Activator    = (*Provider)(nil)
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

// Activate wires the standard v1 snapshot lifecycle for YT sources:
// verify include directives, drop pre-existing target tables, then upload.
// YT is snapshot-only — the replication branch does nothing.
func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if p.transfer.IncrementOnly() {
		return nil
	}
	if _, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination); ok {
		// Copy transfers run the legacy abstract2 upload flow (UploadV2 in the
		// worker). Mirror the pre-activator behaviour: destination cleanup only,
		// no v1 include checks or v1 upload.
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("sink cleanup failed: %w", err)
		}
		return nil
	}
	if err := callbacks.CheckIncludes(tables); err != nil {
		return xerrors.Errorf("include directives check failed: %w", err)
	}
	if err := callbacks.Cleanup(tables); err != nil {
		return xerrors.Errorf("sink cleanup failed: %w", err)
	}
	if err := callbacks.Upload(tables); err != nil {
		return xerrors.Errorf("snapshot loading failed: %w", err)
	}
	return nil
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
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if _, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination); ok {
		// Copy transfers run the legacy abstract2 flow; the legacy abstract1
		// storage is used for the preflight listing only.
		return yt_storage.NewStorage(&provider_yt.YtStorageParams{
			Token:                 src.GetYtToken(),
			Cluster:               src.GetCluster(),
			Path:                  src.GetPaths()[0], // TODO: Handle multi-path in abstract 1 yt storage
			Spec:                  nil,
			DisableProxyDiscovery: src.DisableProxyDiscovery(),
			ConnParams:            src,
		})
	}
	var include []string
	if p.transfer.DataObjects != nil {
		include = p.transfer.DataObjects.IncludeObjects
	}
	s, err := yt_provider.NewSource(p.logger, p.registry, src, include)
	if err != nil {
		return nil, xerrors.Errorf("unable to build yt source: %w", err)
	}
	return s, nil
}

// Target returns the legacy abstract2 event target for copy destinations.
// It is only consulted when the transfer is routed through the abstract2
// flow (IsAbstract2), i.e. for copy destinations only.
func (p *Provider) Target(...abstract.SinkOption) (abstract2.EventTarget, error) {
	dst, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination)
	if !ok {
		return nil, targets.UnknownTargetError
	}
	return target.NewTarget(p.logger, p.registry, dst, p.transfer.ID)
}

// DataProvider returns the legacy abstract2 data provider for copy
// destinations. Non-copy destinations run the v1 flow and never reach here.
func (p *Provider) DataProvider() (provider abstract2.DataProvider, err error) {
	specificConfig, ok := p.transfer.Src.(provider_yt.YtSourceModel)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	if dst, ok := p.transfer.Dst.(*provider_yt.YtCopyDestination); ok {
		return yt_copy_source.NewSource(p.logger, p.registry, specificConfig, dst.SkipLinkFollowing, p.transfer.ID)
	}
	return nil, xerrors.Errorf("abstract2 flow is only supported for copy destinations, got %T", p.transfer.Dst)
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
		sink, err := p.Sink(config)
		if err != nil {
			return nil, xerrors.Errorf("unable to build yt sinker: %w", err)
		}
		return sink, nil
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
