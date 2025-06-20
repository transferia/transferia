//go:build !disable_yt_provider

package provider

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/events"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/provider/dataobjects"
	"github.com/transferia/transferia/pkg/providers/yt/provider/schema"
	"github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

// 16k batches * 2 MiByte per batch should be enough to fill buffer of size 32GiB
const (
	PushBatchSize    = 2 * humanize.MiByte
	MaxInflightCount = 16384 // Max number of successfuly AsyncPush'd batches for which we may wait response from pusher
)

// Parallel table reader settings. These values are taken from YT python wrapper default config
const (
	parallelReadBatchSize = 8 * humanize.MiByte
	parallelTableReaders  = 10
)

type snapshotSource struct {
	cfg  *yt2.YtSource
	yt   yt.Client
	txID yt.TxID
	part *dataobjects.Part

	lgr     log.Logger
	metrics *stats.SourceStats

	lowerIdx uint64
	upperIdx uint64
	totalCnt uint64
	doneCnt  uint64

	isDone    bool
	isStarted bool

	pushQ  chan pushInfo
	readQ  chan *lazyYSON
	stopFn func()
}

type pushInfo struct {
	res  chan error
	rows int
}

func (s *snapshotSource) Start(ctx context.Context, target base.EventTarget) error {
	s.isStarted = true
	defer func() { s.isStarted = false }()
	s.isDone = false

	s.lgr.Debug("Starting snapshot source")
	tbl, err := schema.Load(ctx, s.yt, s.txID, s.part.NodeID(), s.part.Name())
	if err != nil {
		return xerrors.Errorf("error loading table schema: %w", err)
	}
	if s.cfg.RowIdxEnabled() {
		schema.AddRowIdxColumn(tbl, s.cfg.RowIdxColumnName)
	}

	s.lowerIdx = s.part.LowerBound()
	s.upperIdx = s.part.UpperBound()
	s.totalCnt = s.upperIdx - s.lowerIdx
	s.doneCnt = 0

	rowCount, uncSize, err := s.getTableStats(ctx)
	if err != nil {
		return xerrors.Errorf("error reading table attributes: %w", err)
	}
	// Must be impossible case, but let prevent zero division
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

	s.readQ = make(chan *lazyYSON)
	s.pushQ = make(chan pushInfo, MaxInflightCount)

	var errs util.Errors

	readErrCh := s.startReading(ctx, readBatchSizeRows)
	go s.pusher(tbl, target)
	if pushErr := s.consumePushResults(); pushErr != nil {
		errs = util.AppendErr(errs,
			xerrors.Errorf("error pushing events for table %s[%d:%d]: %w",
				s.part.Name(), s.lowerIdx, s.upperIdx, pushErr))
	}
	if readErr := <-readErrCh; readErr != nil {
		errs = util.AppendErr(errs, xerrors.Errorf("error reading table %s[%d:%d]: %w",
			s.part.Name(), s.lowerIdx, s.upperIdx, readErr))
	}

	if len(errs) > 0 {
		return errs
	}

	s.isDone = true
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

func (s *snapshotSource) consumePushResults() error {
	hasErr := false
	var errs util.Errors
	for push := range s.pushQ {
		err := <-push.res
		if err != nil {
			if !hasErr {
				s.stopFn()
				hasErr = true
			}
			errs = util.AppendErr(errs, err)
		} else {
			s.doneCnt += uint64(push.rows)
		}
	}
	if len(errs) > 0 {
		return util.UniqueErrors(errs)
	}
	return nil
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
	var errs util.Errors
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
					if err = s.readTableRange(ctx, rng.lower, rng.upper, stopCh); err != nil {
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
			errs = util.AppendErr(errs, readErr)
		}
	}
	close(s.readQ)
	if len(errs) > 0 {
		return util.UniqueErrors(errs)
	}
	return nil
}

func (s *snapshotSource) pusher(tbl table.YtTable, target base.EventTarget) {
	var batch *batch
	var batchSize int

	partID := fmt.Sprintf("%d_%d", s.lowerIdx, s.upperIdx)

	resetBatch := func(size int) {
		batch = newEmptyBatch(tbl, size, partID, s.cfg.RowIdxColumnName)
		batchSize = 0
	}

	push := func(batch base.EventBatch, cnt int) {
		// trigger mandatory flush if almost MaxInflightCount batches has been pushed
		// and no results has been received or processed
		if (cap(s.pushQ) - len(s.pushQ)) <= 1 {
			s.pushQ <- pushInfo{
				res:  target.AsyncPush(base.NewSingleEventBatch(events.NewDefaultSynchronizeEvent(tbl, partID))),
				rows: 0,
			}
		}
		s.pushQ <- pushInfo{
			res:  target.AsyncPush(batch),
			rows: cnt,
		}
	}

	push(base.NewSingleEventBatch(events.NewDefaultTableLoadEvent(tbl, events.TableLoadBegin).WithPart(partID)), 0)

	resetBatch(100)
	for row := range s.readQ {
		s.metrics.Size.Add(int64(row.RawSize()))

		batch.Append(*row)
		batchSize += row.RawSize()

		if batchSize >= PushBatchSize {
			push(batch, batch.Len())
			resetBatch(batch.Len())
		}
	}
	if lastLen := batch.Len(); lastLen > 0 {
		push(batch, lastLen)
	}

	push(base.NewSingleEventBatch(events.NewDefaultTableLoadEvent(tbl, events.TableLoadEnd).WithPart(partID)), 0)
	close(s.pushQ)
}

func (s *snapshotSource) Running() bool {
	return s.isStarted && !s.isDone
}

func (s *snapshotSource) Stop() error {
	if s.stopFn != nil {
		s.stopFn()
	}
	return nil
}

func (s *snapshotSource) Progress() (base.EventSourceProgress, error) {
	return base.NewDefaultEventSourceProgress(s.isDone, s.doneCnt, s.totalCnt), nil
}

func NewSnapshotSource(cfg *yt2.YtSource, ytc yt.Client, part *dataobjects.Part,
	lgr log.Logger, metrics *stats.SourceStats,
) *snapshotSource {
	return &snapshotSource{
		cfg:       cfg,
		yt:        ytc,
		txID:      part.TxID(),
		part:      part,
		lgr:       lgr,
		metrics:   metrics,
		lowerIdx:  0,
		upperIdx:  0,
		totalCnt:  0,
		doneCnt:   0,
		isDone:    false,
		isStarted: false,
		pushQ:     nil,
		readQ:     nil,
		stopFn:    nil,
	}
}
