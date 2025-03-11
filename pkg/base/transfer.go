package base

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

type Event interface {
	// TODO: fix this, it should be sealed.
}

func EventToString(event Event) string {
	if stringer, ok := event.(fmt.Stringer); ok {
		return stringer.String()
	}
	return fmt.Sprintf("%T", event)
}

type EventBatch interface {
	Next() bool
	Event() (Event, error)
	Count() int
	Size() int
}

type defaultEventBatch struct {
	events []Event
	iter   int
}

func (d *defaultEventBatch) Next() bool {
	if len(d.events)-1 > d.iter {
		d.iter++
		return true
	}
	return false
}

func (d *defaultEventBatch) Event() (Event, error) {
	return d.events[d.iter], nil
}

func (d *defaultEventBatch) Count() int {
	return len(d.events)
}

func (d *defaultEventBatch) Size() int {
	return binary.Size(d.events)
}

func NewEventBatch(events []Event) EventBatch {
	return &defaultEventBatch{events: events, iter: -1}
}

func NewSingleEventBatch(event Event) EventBatch {
	return NewEventBatch([]Event{event})
}

type iteratorOverBatched struct {
	batches []EventBatch
	iter    int
}

func (b *iteratorOverBatched) Next() bool {
	if b.iter == -1 {
		b.iter++
		return b.Next()
	}
	if len(b.batches) > b.iter {
		if b.batches[b.iter].Next() {
			return true
		}
		b.iter++
		return b.Next()
	}
	return false
}

func (b *iteratorOverBatched) Event() (Event, error) {
	return b.batches[b.iter].Event()
}

func (b *iteratorOverBatched) Count() int {
	var cnt int
	for _, batch := range b.batches {
		cnt += batch.Count()
	}
	return cnt
}

func (b *iteratorOverBatched) Size() int {
	var cnt int
	for _, batch := range b.batches {
		cnt += batch.Size()
	}
	return cnt
}

func NewBatchFromBatches(batches []EventBatch) EventBatch {
	return &iteratorOverBatched{batches: batches, iter: -1}
}

// Support legacy
type SupportsOldChangeItem interface {
	Event
	ToOldChangeItem() (*abstract.ChangeItem, error)
}

type LogPosition interface {
	fmt.Stringer
	Equals(otherPosition LogPosition) bool
	Compare(otherPosition LogPosition) (int, error)
}

// Support legacy
type SupportsOldLSN interface {
	LogPosition
	ToOldLSN() (uint64, error) // To ChangeItem.LSN
}

// Support legacy
type SupportsOldCommitTime interface {
	LogPosition
	ToOldCommitTime() (uint64, error) // To ChangeItem.CommitTime
}

type LoggedEvent interface {
	Event
	Position() LogPosition
}

type Transaction interface {
	fmt.Stringer
	BeginPosition() LogPosition
	EndPosition() LogPosition
	Equals(otherTransaction Transaction) bool
}

type TransactionalEvent interface {
	LoggedEvent
	Transaction() Transaction
}

// For any event source, replication, snapshot, etc
type EventSource interface {
	Running() bool
	Start(ctx context.Context, target EventTarget) error
	Stop() error
}

type Discoverable interface {
	Discover() (*model.DataObjects, error)
}

// Reporting of progress for not endless event sources
// Like snapshots, schema creation, etc
type ProgressableEventSource interface {
	EventSource
	Progress() (EventSourceProgress, error)
}

type EventSourceProgress interface {
	Done() bool
	Current() uint64
	Total() uint64
}

type DefaultEventSourceProgress struct {
	done    bool
	current uint64
	total   uint64
}

func NewDefaultEventSourceProgress(done bool, current uint64, total uint64) *DefaultEventSourceProgress {
	return &DefaultEventSourceProgress{
		done:    done,
		current: current,
		total:   total,
	}
}

func (progress *DefaultEventSourceProgress) Done() bool {
	return progress.done
}

func (progress *DefaultEventSourceProgress) Current() uint64 {
	return progress.current
}

func (progress *DefaultEventSourceProgress) Total() uint64 {
	return progress.total
}

// abstract.AsyncSink for abstract2
type EventTarget interface {
	io.Closer
	AsyncPush(input EventBatch) chan error
}

type DataProvider interface {
	Init() error
	Ping() error
	Close() error
}

type SnapshotProvider interface {
	DataProvider
	BeginSnapshot() error
	DataObjects(filter DataObjectFilter) (DataObjects, error)
	TableSchema(part DataObjectPart) (*abstract.TableSchema, error)
	CreateSnapshotSource(part DataObjectPart) (ProgressableEventSource, error)
	EndSnapshot() error

	// Support legacy
	ResolveOldTableDescriptionToDataPart(tableDesc abstract.TableDescription) (DataObjectPart, error)

	// Support new snapshot
	DataObjectsToTableParts(filter DataObjectFilter) ([]abstract.TableDescription, error)
	TablePartToDataObjectPart(tableDescription *abstract.TableDescription) (DataObjectPart, error)
}

func DataObjectsToTableParts(objects DataObjects, filter DataObjectFilter) ([]abstract.TableDescription, error) {
	defer objects.Close()

	tableDescriptions := []abstract.TableDescription{}
	for objects.Next() {
		object, err := objects.Object()
		if err != nil {
			return nil, xerrors.Errorf("Can't get data object: %w", err)
		}
		defer object.Close()

		for object.Next() {
			part, err := object.Part()
			if err != nil {
				return nil, xerrors.Errorf("Can't get data object part: %w", err)
			}

			tableDescription, err := part.ToTablePart()
			if err != nil {
				return nil, xerrors.Errorf("Can't convert part to table description: %w", err)
			}

			tableDescriptions = append(tableDescriptions, *tableDescription)
		}
		if object.Err() != nil {
			return nil, xerrors.Errorf("Can't iterate data object: %w", err)
		}
	}
	if objects.Err() != nil {
		return nil, xerrors.Errorf("Can't iterate data objects: %w", objects.Err())
	}

	return tableDescriptions, nil
}

type ReplicationProvider interface {
	DataProvider
	CreateReplicationSource() (EventSource, error)
}

type TrackerProvider interface {
	DataProvider
	ResetTracker(typ abstract.TransferType) error
}
