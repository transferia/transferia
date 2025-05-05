package providers

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/middlewares"
	"go.ytsaurus.tech/library/go/core/log"
)

// Provider is a bare minimal implementation of provider that can do anything except existing.
type Provider interface {
	Type() abstract.ProviderType
}

// Snapshot add to provider `abstract.Storage` factory to provider.
// this means that provider can read historycal snapshots of data
type Snapshot interface {
	Provider
	Storage() (abstract.Storage, error)
}

// Replication add to provider `abstract.Source` factory to provider.
// this means that provider can do data replication
type Replication interface {
	Provider
	Source() (abstract.Source, error)
}

// Abstract2Provider add `base.DataProvider` factory to provider.
// this means that provider can do abstract2 data provider
type Abstract2Provider interface {
	Provider
	DataProvider() (base.DataProvider, error)
}

// Abstract2Sinker add abstract2 writer factory to provider
type Abstract2Sinker interface {
	Provider
	Target(...abstract.SinkOption) (base.EventTarget, error)
}

// Sinker add generic writer factory to provider
type Sinker interface {
	Provider
	Sink(config middlewares.Config) (abstract.Sinker, error)
}

// SnapshotSinker optional separate writer for snapshots. Will always called for snapshots with all control events
type SnapshotSinker interface {
	Provider
	SnapshotSink(config middlewares.Config) (abstract.Sinker, error)
}

// AsyncSinker add ability to setup async-sink instead of sync-sink for provider
type AsyncSinker interface {
	Provider
	AsyncSink(middleware abstract.Middleware) (abstract.AsyncSink, error)
}

// Sampleable add ability to run `Checksum` to provider.
type Sampleable interface {
	Provider
	SourceSampleableStorage() (abstract.SampleableStorage, []abstract.TableDescription, error)
	DestinationSampleableStorage() (abstract.SampleableStorage, error)
}

type ProviderFactory func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) Provider

var knownProviders = map[abstract.ProviderType]ProviderFactory{}

// Register add new provider factory to known providers registry.
func Register(providerType abstract.ProviderType, fac ProviderFactory) {
	knownProviders[providerType] = fac
}

// Source resolve a specific provider interface from registry by `transfer.SrcType()` provider type.
func Source[T Provider](lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) (T, bool) {
	var defRes T
	f, ok := knownProviders[transfer.SrcType()]
	if !ok {
		return defRes, false
	}
	res := f(lgr, registry, cp, transfer)
	typedRes, ok := res.(T)
	return typedRes, ok
}

// Source implements a specific provider interface from registry
func SourceIs[T Provider](transfer *model.Transfer) bool {
	return ProviderIs[T](transfer.SrcType())
}

// Destination implements a specific provider interface from registry
func DestinationIs[T Provider](transfer *model.Transfer) bool {
	return ProviderIs[T](transfer.DstType())
}

func ProviderIs[T Provider](provider abstract.ProviderType) bool {
	f, ok := knownProviders[provider]
	if !ok {
		return false
	}
	res := f(logger.Log, nil, coordinator.NewFakeClient(), new(model.Transfer))
	_, ok = res.(T)
	return ok
}

// Destination resolve a specific provider interface from registry by `transfer.DstType()` provider type.
func Destination[T Provider](lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) (T, bool) {
	var defRes T
	f, ok := knownProviders[transfer.DstType()]
	if !ok {
		return defRes, false
	}
	res := f(lgr, registry, cp, transfer)
	typedRes, ok := res.(T)
	return typedRes, ok
}
