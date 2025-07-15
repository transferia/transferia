package greenplum

import (
	"context"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/util"
	mathutil "github.com/transferia/transferia/pkg/util/math"
	"go.ytsaurus.tech/library/go/core/log"
)

type Sink struct {
	sinks      pgSinks
	sinkParams *PgSinkParamsRegulated
	// segment pointer -> row ChangeItems for this segment
	rowChangeItems map[GPSegPointer][]abstract.ChangeItem
	// SegPoolShare is the share of segments (from their total count) used by this sink
	SegPoolShare float64
	segPool      *SegPointerPool

	atReplication bool
}

type segPointerOrError struct {
	segment *GPSegPointer
	err     error
}

func newSink(dst *GpDestination, registry metrics.Registry, lgr log.Logger, transferID string, atReplication bool) *Sink {
	accessor := NewStorage(dst.ToGpSource(), registry)
	return &Sink{
		sinks:          newPgSinks(accessor, lgr, transferID, registry),
		sinkParams:     GpDestinationToPgSinkParamsRegulated(dst),
		rowChangeItems: make(map[GPSegPointer][]abstract.ChangeItem),
		SegPoolShare:   0.166,
		segPool:        nil,

		atReplication: atReplication,
	}
}

func NewSink(transfer *model.Transfer, registry metrics.Registry, lgr log.Logger, config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := transfer.Dst.(*GpDestination)
	if !ok {
		return nil, abstract.NewFatalError(xerrors.Errorf("cannot construct GP sink from destination of type %T", transfer.Dst))
	}
	sink := newSink(dst, registry, lgr, transfer.ID, config.ReplicationStage)
	var result abstract.Sinker = sink

	return result, nil
}

func (s *Sink) Close() error {
	if err := s.sinks.Close(); err != nil {
		return xerrors.Errorf("failed while closing Greenplum sink: %w", err)
	}
	return nil
}

func (s *Sink) Push(input []abstract.ChangeItem) error {
	ctx := context.Background()

	if s.atReplication {
		if err := s.replicationPush(ctx, input); err != nil {
			return xerrors.Errorf("failed to push to Greenplum sink at replication: %w", err)
		}
	} else {
		if err := s.snapshotPush(ctx, input); err != nil {
			return xerrors.Errorf("failed to push to Greenplum sink at snapshot: %w", err)
		}
	}
	return nil
}

func (s *Sink) replicationPush(ctx context.Context, input []abstract.ChangeItem) error {
	return s.pushChangeItemsToSegment(ctx, Coordinator(), input)
}

func (s *Sink) snapshotPush(ctx context.Context, input []abstract.ChangeItem) error {
	for i, changeItem := range input {
		if err := s.processSingleChangeItem(ctx, &changeItem); err != nil {
			return xerrors.Errorf("failed to process ChangeItem of kind %q (table %s, #%d in a batch of %d): %w", changeItem.Kind, changeItem.PgName(), i, len(input), err)
		}
	}
	if err := s.flushRowChangeItems(ctx); err != nil {
		return xerrors.Errorf("failed to flush rows: %w", err)
	}

	return nil
}

func (s *Sink) processSingleChangeItem(ctx context.Context, changeItem *abstract.ChangeItem) error {
	if changeItem.IsRowEvent() {
		if err := s.processRowChangeItem(ctx, changeItem); err != nil {
			return xerrors.Errorf("sinker failed to process row: %w", err)
		}
		return nil
	}
	switch changeItem.Kind {
	case abstract.InitShardedTableLoad:
		if err := s.processInitTableLoad(ctx, changeItem); err != nil {
			return xerrors.Errorf("sinker failed to initialize table load for table %s: %w", changeItem.PgName(), err)
		}
	case abstract.InitTableLoad, abstract.SynchronizeKind:
		return nil // do nothing
	case abstract.DoneShardedTableLoad:
		if err := s.processDoneTableLoad(ctx, changeItem); err != nil {
			return xerrors.Errorf("sinker failed to finish table load for table %s: %w", changeItem.PgName(), err)
		}
	case abstract.DoneTableLoad:
		if err := s.flushRowChangeItems(ctx); err != nil {
			return xerrors.Errorf("failed to flush rows: %w", err)
		}
	case abstract.DropTableKind, abstract.TruncateTableKind:
		if err := s.processCleanupChangeItem(ctx, changeItem); err != nil {
			return xerrors.Errorf("failed to process %s: %w", changeItem.Kind, err)
		}
	default:
		return xerrors.Errorf("ChangeItems of kind %q are not supported by Greenplum sink. ChangeItem content: %v", changeItem.Kind, changeItem)
	}
	return nil
}

func (s *Sink) processRowChangeItem(ctx context.Context, changeItem *abstract.ChangeItem) error {
	if changeItem.Kind == abstract.InsertKind {
		// for INSERT, pure on-segment operation is possible
		seg, err := s.chooseSegFromPool(ctx)
		if err != nil {
			return xerrors.Errorf("failed to determine a segment for an item: %w", err)
		}
		setTemporaryTableForChangeItem(changeItem)
		s.rowChangeItems[seg] = append(s.rowChangeItems[seg], *changeItem)
		return nil
	}

	// for all other kinds of ChangeItems, distributed modification of the target table is required
	// so we do not even bother with on-segment operations
	s.rowChangeItems[Coordinator()] = append(s.rowChangeItems[Coordinator()], *changeItem)
	setTemporaryTableForChangeItem(changeItem)
	s.rowChangeItems[Coordinator()] = append(s.rowChangeItems[Coordinator()], *changeItem)

	return nil
}

// setTemporaryTableForChangeItem sets the temporary table as a target for the given ChangeItem.
// If an error is returned, ChangeItem is left unchanged
func setTemporaryTableForChangeItem(changeItem *abstract.ChangeItem) {
	changeItem.Schema, changeItem.Table = temporaryTable(changeItem.Schema, changeItem.Table)
}

func (s *Sink) chooseSegFromPool(ctx context.Context) (GPSegPointer, error) {
	if err := s.ensureSegPoolInitialized(ctx); err != nil {
		return Coordinator(), xerrors.Errorf("failed to initialize a pool of randomly selected segments: %w", err)
	}
	return s.segPool.NextRoundRobin(), nil
}

func (s *Sink) ensureSegPoolInitialized(ctx context.Context) error {
	if s.segPool != nil {
		return nil
	}
	totalSegments, err := s.sinks.TotalSegments(ctx)
	if err != nil {
		return xerrors.Errorf("failed to get the total number of segments: %w", err)
	}
	s.segPool = NewRandomSegPointerPool(totalSegments, mathutil.Max(int(float64(totalSegments)*s.SegPoolShare), 1))
	return nil
}

func (s *Sink) flushRowChangeItems(ctx context.Context) error {
	if err := s.flushRowChangeItemsToSegments(ctx); err != nil {
		return xerrors.Errorf("failed to flush to segments: %w", err)
	}
	// coordinator MUST be flushed after all segments because it may contain modifying operations on rows INSERTed in on-segment mode
	if err := s.flushRowChangeItemsToCoordinator(ctx); err != nil {
		return xerrors.Errorf("failed to flush to %s: %w", Coordinator(), err)
	}
	logger.Log.Debug("Rows flushed to all segments successfully")
	return nil
}

func (s *Sink) flushRowChangeItemsToSegments(ctx context.Context) error {
	outputChan := make(chan segPointerOrError, len(s.rowChangeItems))
	var wg sync.WaitGroup

	for seg := range s.rowChangeItems {
		if seg == Coordinator() {
			continue
		}

		wg.Add(1)
		go func(seg GPSegPointer) {
			defer wg.Done()
			if err := s.pushChangeItemsToSegment(ctx, seg, s.rowChangeItems[seg]); err != nil {
				outputChan <- segPointerOrError{
					segment: nil,
					err:     xerrors.Errorf("failed to push row ChangeItems to %s: %w", seg.String(), err),
				}
			} else {
				outputChan <- segPointerOrError{
					segment: &seg,
					err:     nil,
				}
			}
		}(seg)
	}

	wg.Wait()
	close(outputChan)

	errs := util.NewErrs()
	for el := range outputChan {
		if el.segment != nil {
			delete(s.rowChangeItems, *el.segment)
		}
		errs = util.AppendErr(errs, el.err)
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (s *Sink) flushRowChangeItemsToCoordinator(ctx context.Context) error {
	coordinator := Coordinator()
	toPushChangeItems, ok := s.rowChangeItems[coordinator]
	if !ok {
		return nil
	}
	if err := s.pushChangeItemsToSegment(ctx, coordinator, toPushChangeItems); err != nil {
		return xerrors.Errorf("failed to push row ChangeItems: %w", err)
	}
	delete(s.rowChangeItems, coordinator)
	return nil
}

func (s *Sink) pushChangeItemsToSegment(ctx context.Context, seg GPSegPointer, changeItems []abstract.ChangeItem) error {
	sinker, err := s.sinks.PGSink(ctx, seg, *s.sinkParams)
	if err != nil {
		return xerrors.Errorf("failed to connect to %s: %w", seg.String(), err)
	}
	if err := sinker.Push(changeItems); err != nil {
		return xerrors.Errorf("failed to execute push to %s: %w", seg.String(), err)
	}
	return nil
}

// processCleanupChangeItem flushes ChangeItems and pushes the given one to coordinator
func (s *Sink) processCleanupChangeItem(ctx context.Context, changeItem *abstract.ChangeItem) error {
	if err := s.flushRowChangeItems(ctx); err != nil {
		return xerrors.Errorf("failed to flush rows: %w", err)
	}
	if s.sinkParams.CleanupMode() == model.DisabledCleanup {
		return nil
	}
	if err := s.pushChangeItemsToSegment(ctx, Coordinator(), []abstract.ChangeItem{*changeItem}); err != nil {
		return xerrors.Errorf("failed to execute single push on sinker %s: %w", Coordinator().String(), err)
	}
	return nil
}
