//go:build !disable_yt_provider

package provider

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const ReadRetries = 5

type readerWrapper struct {
	currentIdx uint64
	upperIdx   uint64
	reader     yt.TableReader
	txID       yt.TxID
	lgr        log.Logger
	yt         yt.TableClient
	ctx        context.Context
	tblPath    ypath.Path
}

func (r *readerWrapper) init() error {
	if r.reader != nil {
		return nil
	}
	rd, err := r.yt.ReadTable(r.ctx, r.batchPath(), &yt.ReadTableOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: r.txID},
	})
	if err != nil {
		return xerrors.Errorf("error (re)creating table reader: %w", err)
	}
	r.reader = rd
	return nil
}

func (r *readerWrapper) Close() {
	if r.reader != nil {
		r.reader.Next() // Closing reader without exhausting it causes errors in logs
		_ = r.reader.Close()
		r.reader = nil
	}
}

func (r *readerWrapper) batchPath() *ypath.Rich {
	rng := ypath.Interval(ypath.RowIndex(int64(r.currentIdx)), ypath.RowIndex(int64(r.upperIdx)))
	return r.tblPath.Rich().AddRange(rng)
}

func (r *readerWrapper) Row() (*lazyYSON, error) {
	return backoff.RetryNotifyWithData(func() (*lazyYSON, error) {
		if err := r.ctx.Err(); err != nil {
			//nolint:descriptiveerrors
			return nil, backoff.Permanent(xerrors.Errorf("reader context error: %w", err))
		}

		var rb util.Rollbacks
		defer rb.Do()

		if err := r.init(); err != nil {
			// error is self-descriptive, so no reason to wrap it
			//nolint:descriptiveerrors
			return nil, err
		}
		rb.Add(r.Close)

		if !r.reader.Next() {
			if err := r.reader.Err(); err != nil {
				return nil, xerrors.Errorf("reader error: %w", err)
			} else {
				return nil, xerrors.New("reader exhausted")
			}
		}

		var data lazyYSON
		if err := r.reader.Scan(&data); err != nil {
			return nil, xerrors.Errorf("scan error: %w", err)
		}
		data.rowIDX = int64(r.currentIdx)
		r.currentIdx++

		rb.Cancel()
		return &data, nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), ReadRetries),
		util.BackoffLoggerWarn(r.lgr, "error reading from YT"))
}

func (s *snapshotSource) readTableRange(
	ctx context.Context,
	lowerIdx, upperIdx uint64,
	stopCh <-chan bool,
) error {
	rd := readerWrapper{
		ctx:        ctx,
		tblPath:    s.part.NodeID().YPath(),
		currentIdx: lowerIdx,
		upperIdx:   upperIdx,
		reader:     nil,
		txID:       s.txID,
		lgr:        s.lgr,
		yt:         s.yt,
	}
	defer rd.Close()

	rowCount := upperIdx - lowerIdx
	s.lgr.Debugf("Init reader for %d:%d", lowerIdx, upperIdx)
	for i := uint64(0); i < rowCount; i++ {
		row, err := rd.Row()
		if err != nil {
			return xerrors.Errorf("error reading row %d of %d: %w", rd.currentIdx, rd.upperIdx, err)
		}
		select {
		case <-stopCh:
			return nil
		case s.readQ <- row:
			continue
		}
	}
	s.lgr.Debugf("Done reader for %d:%d", lowerIdx, upperIdx)
	return nil
}
