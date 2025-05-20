package model

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
)

type EndpointParams interface {
	GetProviderType() abstract.ProviderType
	Validate() error

	// WithDefaults sets default values for MISSING parameters of the endpoint
	WithDefaults()
}

type Source interface {
	EndpointParams
	IsSource()
}

type Describable interface {
	Describe() Doc
}

type Doc struct {
	Usage   string `json:"usage,omitempty"`
	Example string `json:"example,omitempty"`
}

type Destination interface {
	EndpointParams
	CleanupMode() CleanupType
	IsDestination()
}

type AlterableDestination interface {
	Destination
	IsAlterable()
}

type SystemTablesDependantDestination interface {
	Destination
	ReliesOnSystemTablesTransferring() bool
}

type ManagedEndpoint interface {
	EndpointParams
	ClusterID() string
}

type Serializeable interface {
	Params() string
	SetParams(string) error
}

// Bellow `marker` interfaces, they used as static mark for certain endpoint types so our code could classify them

type StrictSource interface {
	IsStrictSource()
}

func IsStrictSource(src Source) bool {
	_, ok := src.(StrictSource)
	return ok
}

type Clusterable interface {
	MDBClusterID() string
}

type WithConnectionID interface {
	GetConnectionID() string
}

// Abstract2Source if implemented we must try to create abstract2 source for them
// to be deleted in favor of `MakeDataProvider` method
type Abstract2Source interface {
	IsAbstract2(Destination) bool
}

// IncrementalSource mark as enable incremental snapshot to transfer
type IncrementalSource interface {
	IsIncremental()
	// SupportsStartCursorValue should return true if incremental source may use custom initial cursor value
	SupportsStartCursorValue() bool
}

// TransitionalEndpoint Mark endpoint as type as transitional, so it could be opt-out-ed on snapshot
type TransitionalEndpoint interface {
	TransitionalWith(right TransitionalEndpoint) bool
}

// AppendOnlySource marks a source that can emit only InsertKind. It's queues (kafka/yds/eventhub/lb) with non-CDC parser
// Snapshot-only sources (gp/ch/yt/ydb) isn't AppendOnlySource, bcs of non-row events
// Queues without parser (mirror mode) are not AppendOnlySource
//
// The contract for this interface (use IsAppendOnlySource() to check if source is appendOnly, to make it easy):
//   - If source is not AppendOnlySource - it can emit updates/deletes
//   - If source is AppendOnlySource - then we should call method IsAppendOnly() to determine can it emit updates/deletes
//     (for queues-sources some parsers can emit updates/deletes (who carry CDC events), and other can't)
type AppendOnlySource interface {
	IsAppendOnly() bool
}

func IsAppendOnlySource(src Source) bool {
	if appendOnlySrc, ok := src.(AppendOnlySource); ok {
		return appendOnlySrc.IsAppendOnly()
	} else {
		return false
	}
}

// DefaultMirrorSource marks source as compatible with default mirror protocol (kafka/yds/eventhub)
type DefaultMirrorSource interface {
	IsDefaultMirror() bool
}

func IsDefaultMirrorSource(src Source) bool {
	if defaultMirrorSource, ok := src.(DefaultMirrorSource); ok {
		return defaultMirrorSource.IsDefaultMirror()
	} else {
		return false
	}
}

// Parseable provider unified access to parser config
type Parseable interface {
	Parser() map[string]interface{}
}

func IsParseable(src Source) bool {
	_, ok := src.(Parseable)
	return ok
}

// Serializable provider unified access to serializer config
type Serializable interface {
	Serializer() (SerializationFormat, bool)
}

// Runtimeable will force specific runtime for endpoint
// see logfeller for example
type Runtimeable interface {
	Runtime() abstract.Runtime
}

// Dashboardeable will force specific dashboard link for endpoint pair
// see logfeller for example
type Dashboardeable interface {
	DashboardLink(linkedEndpoint EndpointParams) string
}

// SourceCompatibility for destination to check is it compatible with transfer source
type SourceCompatibility interface {
	Compatible(src Source, transferType abstract.TransferType) error
}

// DestinationCompatibility for source to check is it compatible with transfer destination
type DestinationCompatibility interface {
	Compatible(dst Destination) error
}

// AsyncPartSource designates that source:
//  1. Provides full and correct sequence of table loading events
//     the sequence InitTableLoad -> [InitShardedTableLoad -> RowEvents -> DoneShardedTableLoad] x Shards -> DoneTableLoad for each table
//  2. Can work with true AsyncSink where flush is only guaranteed on Done[Sharded]TableLoad event
//  3. Provides correct PartID in ChangeItem if table is split into sharded parts.
type AsyncPartSource interface {
	IsAsyncShardPartsSource()
}

// UnderlayOnlyEndpoint marks endpoint as available only via our service underlay network
type UnderlayOnlyEndpoint interface {
	IsUnderlayOnlyEndpoint()
}

// HackableTarget hack target model and return rollback for that hack, used only by mysql and yt sources (for test mostly)
// plz don't use it.
type HackableTarget interface {
	PreSnapshotHacks()
	PostSnapshotHacks()
}

// LegacyFillDependentFields for cp-dp backward compatibility, some fields were calculated on backend side before, so we preserve it
type LegacyFillDependentFields interface {
	FillDependentFields(transfer *Transfer)
}

// HostResolver returns a list of hosts to which network availability is required
type HostResolver interface {
	HostsNames() ([]string, error)
}

// For default every destination is support sharding tables, but this interface can be used to override this behavior
type ShardeableDestination interface {
	SupportSharding() bool
}

func IsShardeableDestination(dst Destination) bool {
	if shardeableDestination, ok := dst.(ShardeableDestination); ok {
		return shardeableDestination.SupportSharding()
	} else {
		return true
	}
}

type MultiYtEnabled interface {
	MultiYtEnabled()
}

type SnapshotParallelizationSupport interface {
	SupportMultiWorkers() bool
	SupportMultiThreads() bool
}

type ExtraTransformableSource interface {
	Source
	ExtraTransformers(ctx context.Context, transfer *Transfer, registry metrics.Registry) ([]abstract.Transformer, error)
}

type defaultParallelizationSupport struct{}

func (p defaultParallelizationSupport) SupportMultiWorkers() bool {
	return true
}

func (p defaultParallelizationSupport) SupportMultiThreads() bool {
	return true
}

func GetSnapshotParallelizationSupport(params EndpointParams) SnapshotParallelizationSupport {
	if parallelization, ok := params.(SnapshotParallelizationSupport); ok {
		return parallelization
	}
	return defaultParallelizationSupport{}
}
