package provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type readerWrapper struct {
	currentIdx uint64
	reader     yt.TablePartitionReader
	txID       yt.TxID
	yt         yt.TableClient
	ctx        context.Context
	cookie     []byte
	ranges     []ypath.Range
	rangeIndex int

	decoder     *rowDecoder
	skiffFmt    *skiff.Format
	tableSchema *ytschema.Schema
}

func (r *readerWrapper) init() error {
	if r.reader != nil {
		return nil
	}
	opts := &yt.ReadTablePartitionOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: r.txID},
		Format:             *r.skiffFmt,
		TableSchema:        r.tableSchema,
	}
	rd, err := r.yt.ReadTablePartition(r.ctx, r.cookie, opts)
	if err != nil {
		return xerrors.Errorf("error creating table partition reader: %w", err)
	}
	r.reader = rd
	return nil
}

func (r *readerWrapper) Close() {
	if r.reader != nil {
		_ = r.reader.Close()
		r.reader = nil
	}
}

func (r *readerWrapper) advanceRowIndex() {
	r.currentIdx++
	for r.rangeIndex < len(r.ranges) {
		upper := r.ranges[r.rangeIndex].Upper
		if upper == nil || upper.RowIndex == nil || r.currentIdx < uint64(*upper.RowIndex) {
			return
		}
		r.rangeIndex++
		if r.rangeIndex >= len(r.ranges) {
			return
		}
		lower := r.ranges[r.rangeIndex].Lower
		if lower != nil && lower.RowIndex != nil {
			r.currentIdx = uint64(*lower.RowIndex)
			return
		}
	}
}

func (r *readerWrapper) Row() (decodedRow, error) {
	if err := r.ctx.Err(); err != nil {
		return decodedRow{}, xerrors.Errorf("reader context error: %w", err)
	}
	if err := r.init(); err != nil {
		return decodedRow{}, err
	}
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil {
			return decodedRow{}, xerrors.Errorf("reader error: %w", err)
		}
		return decodedRow{}, xerrors.New("reader exhausted")
	}

	values, err := r.decoder.decode(r.reader, r.currentIdx)
	if err != nil {
		return decodedRow{}, xerrors.Errorf("decode error (row=%d): %w", r.currentIdx, err)
	}

	row := decodedRow{
		values:  values,
		rowIDX:  int64(r.currentIdx),
		rawSize: r.decoder.sizeEstimator.estimate(values),
	}
	r.advanceRowIndex()
	return row, nil
}

func (s *snapshotSource) readTablePartition(ctx context.Context, stopCh <-chan bool) error {
	ranges := s.part.Ranges()
	rd := readerWrapper{
		currentIdx:  0,
		ctx:         ctx,
		reader:      nil,
		txID:        s.txID,
		yt:          s.yt,
		cookie:      s.part.Cookie(),
		ranges:      ranges,
		rangeIndex:  0,
		decoder:     s.decoder.cloneForReader(),
		skiffFmt:    s.skiffFmt,
		tableSchema: s.tableSchema,
	}
	if len(ranges) > 0 && ranges[0].Lower != nil && ranges[0].Lower.RowIndex != nil {
		rd.currentIdx = uint64(*ranges[0].Lower.RowIndex)
	}
	defer rd.Close()

	s.lgr.Debugf("Init partition reader for %s", s.part.Name())
	for i := uint64(0); i < s.part.RowCount(); i++ {
		row, err := rd.Row()
		if err != nil {
			return xerrors.Errorf("error reading row %d of %d: %w", i, s.part.RowCount(), err)
		}
		select {
		case <-stopCh:
			return nil
		case s.readQ <- row:
			continue
		}
	}
	s.lgr.Debugf("Done partition reader for %s", s.part.Name())
	return nil
}
