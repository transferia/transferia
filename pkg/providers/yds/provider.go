package yds

import (
	"context"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/parsers"
	parser_audittrailsv1 "github.com/transferia/transferia/pkg/parsers/registry/audittrailsv1"
	"github.com/transferia/transferia/pkg/providers"
	provider_elastic "github.com/transferia/transferia/pkg/providers/elastic"
	provider_opensearch "github.com/transferia/transferia/pkg/providers/opensearch"
	yds_sink "github.com/transferia/transferia/pkg/providers/yds/sink"
	yds_source "github.com/transferia/transferia/pkg/providers/yds/source"
	"github.com/transferia/transferia/pkg/providers/yds/yds_type"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.YDSSource", new(yds_source.YDSSource))
	gobwrapper.RegisterName("*server.YDSDestination", new(yds_sink.YDSDestination))
	model.RegisterSource(YDSProviderType, func() model.LoggableSource {
		return new(yds_source.YDSSource)
	})
	model.RegisterDestination(YDSProviderType, func() model.LoggableDestination {
		return new(yds_sink.YDSDestination)
	})
	abstract.RegisterProviderName(YDSProviderType, "YDS")
	providers.Register(YDSProviderType, New)

	gobwrapper.RegisterName("*server.YDBTopicSource", new(yds_source.YDBTopicSource))
	gobwrapper.RegisterName("*server.YDBTopicDestination", new(yds_sink.YDBTopicDestination))
	model.RegisterSource(YDBTopicProviderType, func() model.LoggableSource {
		return new(yds_source.YDBTopicSource)
	})
	model.RegisterDestination(YDBTopicProviderType, func() model.LoggableDestination {
		return new(yds_sink.YDBTopicDestination)
	})
	abstract.RegisterProviderName(YDBTopicProviderType, "YDB Topic")
	providers.Register(YDBTopicProviderType, New)
}

const YDSProviderType = yds_type.YDSProviderType
const YDBTopicProviderType = yds_type.YDBTopicProviderType

// To verify providers contract implementation
var (
	_ providers.Replication             = (*Provider)(nil)
	_ providers.PartitionableSource     = (*Provider)(nil)
	_ providers.PartitionListerProvider = (*Provider)(nil)
	_ providers.Sinker                  = (*Provider)(nil)

	_ providers.Activator    = (*Provider)(nil)
	_ providers.Deactivator  = (*Provider)(nil)
	_ providers.SrcCleanuper = (*Provider)(nil)
	_ providers.Tester       = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return YDSProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	switch src := p.transfer.Src.(type) {
	case *yds_source.YDSSource:
		src.IsLbSink = p.transfer.DstType() == YDSProviderType

		switch p.transfer.Dst.(type) {
		case *provider_elastic.ElasticSearchDestination, *provider_opensearch.OpenSearchDestination:
			if !parsers.IsThisParserConfig(src.ParserConfig, new(parser_audittrailsv1.ParserConfigAuditTrailsV1Common)) {
				break
			}
			var err error
			src.ParserConfig, err = enableUseElasticSchema(src.ParserConfig)
			if err != nil {
				return nil, xerrors.Errorf("unable to enrich AuditTrails parser with defaults: %w", err)
			}
		}

		source, err := yds_source.NewSource(p.transfer.ID, src, p.logger, p.registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDS source: %w", err)
		}
		return source, nil
	case *yds_source.YDBTopicSource:
		src.IsYDBTopicSink = p.transfer.DstType() == YDBTopicProviderType
		source, err := yds_source.NewSource(p.transfer.ID, src, p.logger, p.registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDB Topic source: %w", err)
		}
		return source, nil
	default:
		return nil, xerrors.Errorf("unknown source type: %T", p.transfer.Src)
	}
}

func (p *Provider) PartitionLister() (abstract.PartitionLister, error) {
	src, ok := p.transfer.Src.(*yds_source.YDSSource)
	if !ok {
		return nil, xerrors.Errorf("unknown partition lister source type: %T", p.transfer.Src)
	}

	return yds_source.NewPartitionLister(p.transfer.ID, src, p.logger)
}

func (p *Provider) PartitionSource(partition abstract.Partition) (abstract.QueueToS3Source, error) {
	src, ok := p.transfer.Src.(*yds_source.YDSSource)
	if !ok {
		return nil, xerrors.Errorf("Unknown partition source type: %T", p.transfer.Src)
	}

	return yds_source.NewPartitionSource(p.transfer.ID, src, partition, p.logger, p.registry)
}

// enableUseElasticSchema enables UseElasticSchema param for YDSSource.ParserConfig.
func enableUseElasticSchema(in map[string]any) (map[string]any, error) {
	abstractCfg, err := parsers.ParserConfigMapToStruct(in)
	if err != nil {
		return nil, xerrors.Errorf("unable to obtain config from map: %w", err)
	}

	cfg, ok := abstractCfg.(*parser_audittrailsv1.ParserConfigAuditTrailsV1Common)
	if !ok {
		return nil, xerrors.Errorf("config expected to be *audittrailsv1.ParserConfigAuditTrailsV1Common, got %T", abstractCfg)
	}
	cfg.UseElasticSchema = true

	out, err := parsers.ParserConfigStructToMap(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to store config as map: %w", err)
	}
	return out, nil
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	switch dst := p.transfer.Dst.(type) {
	case *yds_sink.YDSDestination:
		var err error
		cfgCopy := *dst
		cfgCopy.LbDstConfig.FormatSettings, err = coherence_check.InferFormatSettings(p.logger, p.transfer.Src, cfgCopy.LbDstConfig.FormatSettings)
		if err != nil {
			return nil, xerrors.Errorf("unable to infer format settings: %w", err)
		}

		sink, err := yds_sink.NewSink(&cfgCopy, p.registry, p.logger, p.transfer.ID)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDS sink: %w", err)
		}

		return sink, nil
	case *yds_sink.YDBTopicDestination:
		var err error
		cfgCopy := *dst
		cfgCopy.FormatSettings, err = coherence_check.InferFormatSettings(p.logger, p.transfer.Src, cfgCopy.FormatSettings)
		if err != nil {
			return nil, xerrors.Errorf("unable to infer format settings: %w", err)
		}

		sink, err := yds_sink.NewSink(&cfgCopy, p.registry, p.logger, p.transfer.ID)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDB Topic sink: %w", err)
		}

		return sink, nil
	default:
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
}

const (
	ReadRuleCheck = abstract.CheckType("read-rule-check")
	ConfigCheck   = abstract.CheckType("source-config-check")
)

func (p *Provider) TestChecks() []abstract.CheckType {
	return []abstract.CheckType{ReadRuleCheck, ConfigCheck}
}

func (p *Provider) Test(ctx context.Context) *abstract.TestResult {
	src, ok := p.transfer.Src.(*yds_source.YDSSource)
	if !ok {
		return nil
	}
	tr := abstract.NewTestResult(p.TestChecks()...)
	if src.Consumer == "" {
		src.Consumer = "test-endpoint-" + p.transfer.ID
		defer func() {
			err := DropReadRule(src, "")
			if err != nil {
				p.logger.Error("cannot drop read rule", log.Error(err))
			}
		}()
	}
	err := CreateReadRule(src, "")
	if err != nil {
		return tr.NotOk(ReadRuleCheck, xerrors.Errorf("unable to add read rule: %w", err))
	}
	tr.Ok(ReadRuleCheck)
	currSource, err := p.Source()
	if err != nil {
		return tr.NotOk(ConfigCheck, xerrors.Errorf("unable to construct reader: %w", err))
	}
	tr.Ok(ConfigCheck)
	return tasks.SniffReplicationData(ctx, currSource.(abstract.Fetchable), tr, p.transfer)
}

func (p *Provider) Activate(_ context.Context, _ *model.TransferOperation, _ abstract.TableMap, _ providers.ActivateCallbacks) error {
	if !p.transfer.IncrementOnly() {
		return xerrors.New("only replication mode is allowed for YDS and YDBTopic sources")
	}

	switch src := p.transfer.Src.(type) {
	case *yds_source.YDSSource:
		if err := CreateReadRule(src, p.transfer.ID); err != nil {
			return xerrors.Errorf("unable to add read rule: %w", err)
		}
		return nil
	case *yds_source.YDBTopicSource:
		return nil
	default:
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
}

func (p *Provider) dropNonCustomReadRule() error {
	src, ok := p.transfer.Src.(*yds_source.YDSSource)
	if !ok {
		// Non-YDS sources (e.g. YDB Topic) do not manage read rules.
		return nil
	}
	if src.Consumer != "" {
		p.logger.Infof("skip drop for user defined consumer '%v'", src.Consumer)
		return nil
	}
	return DropReadRule(src, p.transfer.ID)
}

func (p *Provider) CleanupSuitable(transferType abstract.TransferType) bool {
	return transferType != abstract.TransferTypeSnapshotOnly
}

func (p *Provider) CleanupSource(_ context.Context) error {
	return p.dropNonCustomReadRule()
}

func (p *Provider) Deactivate(_ context.Context, _ *model.TransferOperation) error {
	return p.dropNonCustomReadRule()
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
