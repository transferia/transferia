package mongo

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.MongoCollection", new(MongoCollection))
	gobwrapper.RegisterName("*server.MongoSource", new(MongoSource))
	gobwrapper.RegisterName("*server.MongoDestination", new(MongoDestination))
	model.RegisterDestination(ProviderType, func() model.Destination {
		return new(MongoDestination)
	})
	model.RegisterSource(ProviderType, func() model.Source {
		return new(MongoSource)
	})

	abstract.RegisterProviderName(ProviderType, "Mongo")
	providers.Register(ProviderType, New)

	/*
		"__data_transfer":
			Mongo database. Stores __dt_cluster_time in old versions of Mongo.

		"__dt_cluster_time":
			{worker_time time.Time, cluster_time time.Time}
			Mongo collection. Stores timestamps of pings and oplog position for each transfer.
	*/
	abstract.RegisterSystemTables(SystemDatabase, ClusterTimeCollName)
}

const (
	SystemDatabase      = "__data_transfer" // used only for old versions of mongo
	ClusterTimeCollName = "__dt_cluster_time"
)

const ProviderType = abstract.ProviderType("mongo")

// To verify providers contract implementation
var (
	_ providers.Sinker      = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Sampleable  = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*MongoSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Src)
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType
	}
	src.SlotID = p.transfer.ID
	return NewSource(src, p.transfer.ID, p.transfer.DataObjects, p.logger, p.registry, p.cp)
}

func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*MongoDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	return NewSinker(p.logger, dst, p.registry)
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*MongoSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType
	}
	res, err := NewStorage(src.ToStorageParams(), WithMetrics(p.registry))
	if err != nil {
		return nil, xerrors.Errorf("failed to create a MongoDB storage: %w", err)
	}
	res.IsHomo = src.IsHomo
	return res, nil
}

func (p *Provider) SourceSampleableStorage() (abstract.SampleableStorage, []abstract.TableDescription, error) {
	src, ok := p.transfer.Src.(*MongoSource)
	if !ok {
		return nil, nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	srcStorage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to create mongo storage: %w`, err)
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType
	}
	if src.IsHomo {
		// Need to toggle is homo to extract binary primary_key (see ExtractKey in pkg/dataagent/mongo/schema.go)
		srcStorage.IsHomo = true
	}
	all, err := srcStorage.TableList(nil)
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to get table map from source storage: %w`, err)
	}
	var tables []abstract.TableDescription
	for tID, tInfo := range all {
		if src.Include(tID) {
			tables = append(tables, abstract.TableDescription{
				Name:   tID.Name,
				Schema: tID.Namespace,
				Filter: "",
				EtaRow: tInfo.EtaRow,
				Offset: 0,
			})
		}
	}
	return srcStorage, tables, nil
}

func (p *Provider) DestinationSampleableStorage() (abstract.SampleableStorage, error) {
	dst, ok := p.transfer.Dst.(*MongoDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	dstStorageMongo, err := NewStorage(dst.ToStorageParams())
	if err != nil {
		return nil, err
	}
	if p.transfer.SrcType() == ProviderType {
		dstStorageMongo.IsHomo = true
	}
	return dstStorageMongo, nil
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	src, ok := p.transfer.Src.(*MongoSource)
	if !ok {
		return xerrors.Errorf("unexpected source: %T", p.transfer.Src)
	}
	src.SlotID = p.transfer.ID
	if !p.transfer.SnapshotOnly() {
		if err := SyncClusterTime(ctx, src, src.RootCAFiles); err != nil {
			return xerrors.Errorf("Cannot retrieve a timestamp from the MongoDB cluster: %w", err)
		}
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("Sinker cleanup failed: %w", err)
		}
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	}
	return nil
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
