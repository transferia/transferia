package provider

import (
	"context"
	"errors"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yt"
)

// PushBatchSize is the flush threshold (in raw bytes) for accumulating rows
// into a single abstract.Pusher call.
const PushBatchSize = 2 * humanize.MiByte

// synchronizeFlushBytes is the byte budget after which a Synchronize item is
// pushed for the current part, so sinks flush partially loaded data. It
// approximates the legacy abstract2 in-flight window (MaxInflightCount=16384
// batches × PushBatchSize) that the old pusher tracked via its push queue.
// A var so tests can shrink the budget.
var synchronizeFlushBytes = 16384 * PushBatchSize

// snapshotSource loads a single YT SDK table partition and streams the
// decoded rows into an abstract.Pusher. It is instantiated per LoadTable call
// by the outer source; state is not reused between parts.
type snapshotSource struct {
	cfg  provider_yt.YtSourceModel
	yt   yt.Client
	txID yt.TxID
	part *dataobjects.Part

	lgr     log.Logger
	metrics *stats.SourceStats

	lowerIdx uint64
	upperIdx uint64
	totalCnt uint64

	readQ  chan decodedRow
	stopFn func()

	// Populated at the start of loadPart before reader goroutines start.
	decoder     *rowDecoder
	skiffFmt    *skiff.Format
	tableSchema *ytschema.Schema

	columns []string

	// synchronizeFlushBytes is the byte budget between Synchronize flushes;
	// overridable in tests.
	synchronizeFlushBytes int
}

// loadPart drives the whole snapshot pipeline for the assigned part:
// resolve schema -> read SDK partition -> accumulate rows into batches ->
// synchronously flush every full batch through pusher. The TableDescription
// is supplied by the caller (source.LoadTable) so ChangeItem.{Schema, Table,
// PartID} stay identical to what MakeInitTableLoad emits — otherwise the async
// CH sink cannot correlate data rows with the registered part (its part map
// is keyed by TablePartID{TableID, PartID}).
func (s *snapshotSource) loadPart(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	s.lgr.Debug("Starting snapshot source")
	// Single source-of-truth for the table schema seen at data-row time. The
	// same helper is called by source.TableSchema and source.loadTableSchema
	// on the init-event side, so ChangeItem.TableSchema attached to rows here
	// cannot drift from the schema the sink received at CREATE TABLE time.
	idxColName := s.cfg.GetRowIdxColumn()
	tbl, err := resolveYtTable(ctx, s.yt, s.txID, s.part.NodeID(), s.part.Name(), s.columns, idxColName)
	if err != nil {
		return xerrors.Errorf("error loading table schema: %w", err)
	}

	ytSchema := ytSchemaForSkiff(tbl, idxColName)
	s.tableSchema = &ytSchema
	s.skiffFmt = buildSkiffFormat(tbl, idxColName)
	s.decoder = newRowDecoder(tbl, idxColName)

	s.lowerIdx = s.part.LowerBound()
	s.upperIdx = s.part.UpperBound()
	s.totalCnt = s.part.RowCount()

	if s.totalCnt == 0 {
		s.lgr.Warnf("Table %s part [%d:%d] seems to be empty, got row_count = 0", s.part.Name(), s.lowerIdx, s.upperIdx)
		return nil
	}

	s.readQ = make(chan decodedRow)

	var errs []error
	readErrCh := s.startReading(ctx)

	if pushErr := s.pushLoop(tbl, table, pusher); pushErr != nil {
		// Signal readers to stop; the read loop will surface the joined error.
		if s.stopFn != nil {
			s.stopFn()
		}
		errs = append(errs, xerrors.Errorf("error pushing events for table %s[%d:%d]: %w",
			s.part.Name(), s.lowerIdx, s.upperIdx, pushErr))
	}
	// Drain the reader queue if the pusher aborted early — otherwise the reader
	// goroutines block forever on send.
	for range s.readQ {
	}
	if readErr := <-readErrCh; readErr != nil {
		errs = append(errs, xerrors.Errorf("error reading table %s[%d:%d]: %w",
			s.part.Name(), s.lowerIdx, s.upperIdx, readErr))
	}

	return errors.Join(errs...)
}

// pushLoop consumes decoded rows from readQ, batches them by PushBatchSize
// bytes and flushes each batch synchronously through pusher.
//
// After every synchronizeFlushBytes of pushed data a Synchronize item for the
// current part is pushed as well, so sinks flush the partially loaded part
// (intermediate visibility) — the provider-internal replacement for the
// legacy abstract2 pusher, which emitted a Synchronize event when its
// in-flight window (16384 batches) filled up. Only the mid-loop flush is
// emitted: the final part state is flushed by the DoneTableLoad control
// event, as in the legacy flow.
func (s *snapshotSource) pushLoop(tbl yt_table.YtTable, table abstract.TableDescription, pusher abstract.Pusher) error {
	partID := table.GeneratePartID()
	b, err := newEmptyBatch(tbl, 100, table.Schema, table.Name, partID, s.cfg.GetRowIdxColumn())
	if err != nil {
		return xerrors.Errorf("unable to initialize batch: %w", err)
	}
	pushSynchronize := func() error {
		sync := abstract.MakeSynchronizeEvent()
		sync.Schema, sync.Table, sync.PartID = table.Schema, table.Name, partID
		if err := pusher([]abstract.ChangeItem{sync}); err != nil {
			return xerrors.Errorf("unable to push synchronize event: %w", err)
		}
		return nil
	}
	sinceSync := 0
	for row := range s.readQ {
		s.metrics.Size.Add(int64(row.RawSize()))
		b.Append(row)
		if b.Size() >= PushBatchSize {
			batchBytes := b.Size()
			if err := pusher(b.Items()); err != nil {
				return xerrors.Errorf("unable to push batch (mid-loop): %w", err)
			}
			sinceSync += batchBytes
			if sinceSync >= s.synchronizeFlushBytes {
				if err := pushSynchronize(); err != nil {
					return err
				}
				sinceSync = 0
			}
			b, err = newEmptyBatch(tbl, b.Len(), table.Schema, table.Name, partID, s.cfg.GetRowIdxColumn())
			if err != nil {
				return xerrors.Errorf("unable to initialize next batch: %w", err)
			}
		}
	}
	if b.Len() > 0 {
		if err := pusher(b.Items()); err != nil {
			return xerrors.Errorf("unable to push final batch: %w", err)
		}
	}
	return nil
}

func (s *snapshotSource) startReading(ctx context.Context) chan error {
	stopCh := make(chan bool)
	var stopOnce sync.Once
	s.stopFn = func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}
	resCh := make(chan error, 1)

	go func() {
		defer close(s.readQ)
		resCh <- s.readTablePartition(ctx, stopCh)
		close(resCh)
	}()
	return resCh
}

func NewSnapshotSource(cfg provider_yt.YtSourceModel, ytc yt.Client, part *dataobjects.Part,
	lgr log.Logger, metrics *stats.SourceStats, columns []string) *snapshotSource {
	return &snapshotSource{
		cfg:         cfg,
		yt:          ytc,
		txID:        part.TxID(),
		part:        part,
		lgr:         lgr,
		metrics:     metrics,
		lowerIdx:    0,
		upperIdx:    0,
		totalCnt:    0,
		readQ:       nil,
		stopFn:      nil,
		decoder:     nil,
		skiffFmt:    nil,
		tableSchema: nil,
		columns:     columns,

		synchronizeFlushBytes: synchronizeFlushBytes,
	}
}
