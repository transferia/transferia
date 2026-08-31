package provider

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
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

// Parallel table reader settings. These values are taken from YT python wrapper default config
const (
	parallelReadBatchSize = 8 * humanize.MiByte
	parallelTableReaders  = 10
)

// snapshotSource loads a single row range of a YT table and streams the
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
	decoder  *rowDecoder
	skiffFmt *skiff.Format

	columns []string

	// synchronizeFlushBytes is the byte budget between Synchronize flushes;
	// overridable in tests.
	synchronizeFlushBytes int
}

// loadPart drives the whole snapshot pipeline for the assigned part:
// resolve schema -> spawn parallel readers -> accumulate rows into batches ->
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

	s.skiffFmt = buildSkiffFormat(tbl, idxColName)
	s.decoder = newRowDecoder(tbl, idxColName)

	s.lowerIdx = s.part.LowerBound()
	s.upperIdx = s.part.UpperBound()
	s.totalCnt = s.upperIdx - s.lowerIdx

	rowCount, uncSize, err := s.getTableStats(ctx)
	if err != nil {
		return xerrors.Errorf("error reading table attributes: %w", err)
	}
	// Guard against zero division for an empty part.
	if rowCount == 0 {
		s.lgr.Warnf("Table %s part [%d:%d] seems to be empty, got row_count = 0", s.part.Name(), s.lowerIdx, s.upperIdx)
		return nil
	}
	avgRowWeight := float64(uncSize) / float64(rowCount)
	readBatchSizeRows := uint64(math.Ceil(float64(parallelReadBatchSize) / avgRowWeight))
	if readBatchSizeRows > s.totalCnt {
		readBatchSizeRows = s.totalCnt
	}
	s.lgr.Infof("Infer parallel read batch size as %d rows", readBatchSizeRows)

	s.readQ = make(chan decodedRow)

	var errs []error
	readErrCh := s.startReading(ctx, readBatchSizeRows)

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

func (s *snapshotSource) getTableStats(ctx context.Context) (rowCount, uncomprSize int64, err error) {
	var data struct {
		RowCount         int64 `yson:"row_count,attr"`
		UncompressedSize int64 `yson:"uncompressed_data_size,attr"`
	}
	err = s.yt.GetNode(ctx, s.part.NodeID().YPath(), &data, &yt.GetNodeOptions{
		Attributes:         []string{"row_count", "uncompressed_data_size"},
		TransactionOptions: &yt.TransactionOptions{TransactionID: s.txID},
	})
	return data.RowCount, data.UncompressedSize, err
}

func (s *snapshotSource) startReading(ctx context.Context, batchSize uint64) chan error {
	stopCh := make(chan bool)
	var stopOnce sync.Once
	s.stopFn = func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}
	resCh := make(chan error, 1)

	go func() {
		resCh <- s.runReaders(ctx, batchSize, stopCh)
		close(resCh)
	}()
	return resCh
}

func (s *snapshotSource) runReaders(ctx context.Context, batchSize uint64, stopCh <-chan bool) error {
	var errs []error
	type tblRange struct {
		lower uint64
		upper uint64
	}

	ranges := make(chan tblRange, s.totalCnt/batchSize+1)
	for i := s.lowerIdx; i < s.upperIdx; i += batchSize {
		upper := i + batchSize
		if upper > s.upperIdx {
			upper = s.upperIdx
		}
		ranges <- tblRange{i, upper}
	}
	close(ranges)

	readResCh := make(chan error, parallelTableReaders)
	for i := 0; i < parallelTableReaders; i++ {
		go func() {
			var err error
			defer func() { readResCh <- err }()
			for {
				select {
				case rng, ok := <-ranges:
					if !ok {
						return
					}
					if err = s.readTableRange(ctx, rng.lower, rng.upper, stopCh, s.columns); err != nil {
						return
					}
				case <-stopCh:
					return
				}
			}
		}()
	}

	for i := 0; i < parallelTableReaders; i++ {
		readErr := <-readResCh
		if readErr != nil {
			s.stopFn()
			errs = append(errs, readErr)
		}
	}
	close(s.readQ)
	return errors.Join(errs...)
}

func NewSnapshotSource(cfg provider_yt.YtSourceModel, ytc yt.Client, part *dataobjects.Part,
	lgr log.Logger, metrics *stats.SourceStats, columns []string) *snapshotSource {
	return &snapshotSource{
		cfg:      cfg,
		yt:       ytc,
		txID:     part.TxID(),
		part:     part,
		lgr:      lgr,
		metrics:  metrics,
		lowerIdx: 0,
		upperIdx: 0,
		totalCnt: 0,
		readQ:    nil,
		stopFn:   nil,
		decoder:  nil,
		skiffFmt: nil,
		columns:  columns,

		synchronizeFlushBytes: synchronizeFlushBytes,
	}
}
