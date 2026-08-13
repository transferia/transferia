package postgres

import (
	"context"
	"errors"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	queue_to_s3_parsequeue "github.com/transferia/transferia/pkg/parsequeue/queue_to_s3"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type queueToS3Replication struct {
	replication *replication
}

// PartitionSource runs logical WAL replication into a QueueToS3Sink.
type PartitionSource struct {
	*Worker
}

func NewPartitionSource(
	src *PgSource,
	transferID string,
	objects *model.DataObjects,
	partition abstract.Partition,
	lgr log.Logger,
	registry *stats.SourceStats,
	cp coordinator.Coordinator,
) (*PartitionSource, error) {
	if partition.Topic != src.SlotID || partition.Partition != 0 {
		return nil, xerrors.Errorf(
			"unexpected postgres queue-to-s3 partition %s:%d, expected %s:0",
			partition.Topic, partition.Partition, src.SlotID,
		)
	}

	w, err := NewSourceWrapper(src, transferID, objects, lgr, registry, cp, false)
	if err != nil {
		return nil, err
	}
	worker, ok := w.(*Worker)
	if !ok {
		return nil, xerrors.Errorf("unexpected source wrapper type: %T", w)
	}
	return &PartitionSource{Worker: worker}, nil
}

func (s *PartitionSource) Run(sink abstract.QueueToS3Sink) error {
	err := s.runQueueToS3(sink)
	if err == nil {
		s.logger.Info("postgres partition source - run done successfully")
		return nil
	}

	s.logger.Error("postgres partition source - run done (with error)", log.Error(err))
	if abstract.IsFatal(err) {
		if suicideErr := s.slot.Suicide(); suicideErr != nil {
			s.logger.Errorf("slotID.Suicide() returned error: %s", suicideErr.Error())
		}
	}
	return xerrors.Errorf("postgres partition source run failed: %w", err)
}

func (s *PartitionSource) runQueueToS3(sink abstract.QueueToS3Sink) error {
	return s.runPublisher(sink, func(publisher abstract.Source) error {
		replicationPublisher, ok := publisher.(*replication)
		if !ok {
			publisher.Stop()
			return xerrors.Errorf("postgres queue-to-s3 requires logical replication publisher, got %T", publisher)
		}
		return (&queueToS3Replication{replication: replicationPublisher}).run(sink)
	})
}

func (p *queueToS3Replication) run(sink abstract.QueueToS3Sink) error {
	parseQ := queue_to_s3_parsequeue.New(
		p.replication.logger,
		10,
		&ackEmptyQueueToS3Sink{inner: sink},
		p.parse,
		p.ack,
	)

	runErr := p.replication.run(parseQ)
	parseQ.Close()

	if parseQ.Error() != nil {
		runErr = errors.Join(runErr, xerrors.Errorf("parse queue error: %w", parseQ.Error()))
	}
	return runErr
}

func (p *queueToS3Replication) ack(pushResult abstract.QueueResult) error {
	p.replication.mutex.Lock()
	defer p.replication.mutex.Unlock()

	if !util.IsOpen(p.replication.stopCh) {
		return nil
	}
	if len(pushResult.Offsets) == 0 {
		return nil
	}

	committedLsn, err := p.replication.sequencer.PushedOffsets(pushResult.Offsets)
	if err != nil {
		logger.Log.Error("sequence of processed offsets is incorrect", log.Error(err))
		return err
	}
	p.replication.maxLsn = committedLsn
	return nil
}

// parse filters items for the Iceberg/S3 sink and fills QueueMessageMeta.
// Items that are not pushed (include-filter / system tables) are acked immediately so
// the slot sequencer does not stall — classic parsequeue acks the original full batch.
func (p *queueToS3Replication) parse(items []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	filtered, err := p.replication.WithIncludeFilter(items)
	if err != nil {
		return nil, err
	}

	toPush := make([]abstract.ChangeItem, 0, len(filtered))
	for _, item := range filtered {
		if item.IsSystemTable() {
			continue
		}
		toPush = append(toPush, item)
	}

	if excludedOffsets := offsetsNotPushed(items, toPush); len(excludedOffsets) > 0 {
		p.replication.mutex.Lock()
		committedLsn, ackErr := p.replication.sequencer.PushedOffsets(excludedOffsets)
		if ackErr != nil {
			p.replication.mutex.Unlock()
			return nil, xerrors.Errorf("unable to ack filtered offsets: %w", ackErr)
		}
		p.replication.maxLsn = committedLsn
		p.replication.mutex.Unlock()
	}

	for i := range toPush {
		toPush[i].FillQueueMessageMeta(p.replication.config.SlotID, 0, toPush[i].LSN, toPush[i].Counter)
	}
	return toPush, nil
}

// offsetsNotPushed returns LSN occurrences from all that are not present in pushed,
// preserving multiplicity (same LSN may appear multiple times within one XLogData).
func offsetsNotPushed(all, pushed []abstract.ChangeItem) []uint64 {
	remaining := make(map[uint64]int, len(pushed))
	for _, item := range pushed {
		remaining[item.LSN]++
	}
	excluded := make([]uint64, 0)
	for _, item := range all {
		if count := remaining[item.LSN]; count > 0 {
			remaining[item.LSN] = count - 1
			continue
		}
		excluded = append(excluded, item.LSN)
	}
	return excluded
}

// ackEmptyQueueToS3Sink acks empty batches immediately. Some QueueToS3 sinks
// (e.g. Iceberg) ignore empty pushes and never send a result, which would hang
// the queue-to-s3 parsequeue after fully filtered WAL batches.
type ackEmptyQueueToS3Sink struct {
	inner abstract.QueueToS3Sink
}

func (s *ackEmptyQueueToS3Sink) AsyncV2Push(ctx context.Context, errCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	if len(items) == 0 {
		errCh <- &abstract.QueueSourceAsyncPushResult{
			Result: abstract.QueueResult{Offsets: nil},
			Err:    nil,
		}
		return
	}
	s.inner.AsyncV2Push(ctx, errCh, items)
}

func (s *ackEmptyQueueToS3Sink) Close() error {
	return s.inner.Close()
}
