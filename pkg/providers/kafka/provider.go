package kafka

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	destinationFactory := func() model.Destination {
		return new(KafkaDestination)
	}
	gobwrapper.RegisterName("*server.KafkaSource", new(KafkaSource))
	gobwrapper.RegisterName("*server.KafkaDestination", new(KafkaDestination))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(KafkaSource)
	})
	model.RegisterDestination(ProviderType, destinationFactory)
	abstract.RegisterProviderName(ProviderType, "Kafka")

	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("kafka")

// To verify providers contract implementation
var (
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sniffer     = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

var systemTopics = set.New(
	"__consumer_offsets", // is used to store information about committed offsets for each topic:partition per group of consumers (groupID).
	"_schema",            // is not a default kafka topic (at least at kafka 8,9). This is an internal topic used by the Schema Registry which is a distributed storage layer for Avro schemas.
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Sniffer(_ context.Context) (abstract.Fetchable, error) {
	src, ok := p.transfer.Src.(*KafkaSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	topics := src.GroupTopics
	if len(topics) == 0 && src.Topic != "" {
		topics = append(topics, src.Topic)
	}
	var err error
	src.Auth.Password, err = ResolvePassword(src.Connection, src.Auth)
	if err != nil {
		return nil, xerrors.Errorf("unable to get password: %w", err)
	}

	if len(topics) == 0 { // no topics specified, we should sniff all topics
		brokers, err := ResolveBrokers(src.Connection)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
		}

		if len(brokers) == 0 {
			return nil, xerrors.New("empty brokers")
		}
		mechanism, err := src.Auth.GetAuthMechanism()
		if err != nil {
			return nil, xerrors.Errorf("unable to define auth mechanism: %w", err)
		}
		tlsCfg, err := src.Connection.TLSConfig()
		if err != nil {
			return nil, xerrors.Errorf("unable to construct tls config: %w", err)
		}
		kafkaClient, err := client.NewClient(brokers, mechanism, tlsCfg, nil)
		if err != nil {
			return nil, xerrors.Errorf("unable to create kafka client, err: %w", err)
		}
		topics, err = kafkaClient.ListTopics()
		if err != nil {
			return nil, xerrors.Errorf("unable to list topics: %w", err)
		}
		topics = yslices.Filter(topics, func(s string) bool {
			return !systemTopics.Contains(s) // ignore system topics
		})
	}
	return &sourceMultiTopics{
		src:      src,
		topics:   topics,
		logger:   p.logger,
		registry: p.registry,
	}, nil
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*KafkaSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if !src.IsHomo { // we can enforce homo from outside
		src.IsHomo = p.transfer.DstType() == ProviderType && src.IsDefaultMirror()
	}
	if !src.SynchronizeIsNeeded {
		src.SynchronizeIsNeeded = p.transfer.DstType() == "lb" // sorry for that
	}
	if len(p.transfer.DataObjects.GetIncludeObjects()) > 0 && len(src.GroupTopics) == 0 { // infer topics from transfer
		src.GroupTopics = p.transfer.DataObjects.GetIncludeObjects()
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*KafkaDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *dst
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewReplicationSink(&cfgCopy, p.registry, p.logger)
}

func (p *Provider) SnapshotSink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*KafkaDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *dst
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewSnapshotSink(&cfgCopy, p.registry, p.logger)
}

func (p *Provider) Activate(_ context.Context, _ *model.TransferOperation, _ abstract.TableMap, _ providers.ActivateCallbacks) error {
	if p.transfer.SrcType() == ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for Kafka source is replication")
	}
	return nil
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
