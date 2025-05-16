package async

import (
	"context"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	memSizeLimit   = 10 * humanize.MiByte
	batchSizeLimit = 100 * humanize.MiByte
)

type chV2Streamer struct {
	conn       clickhouse.Conn
	batch      driver.Batch
	query      string
	memSize    uint64
	batchSize  uint64
	marshaller db.ChangeItemMarshaller
	lgr        log.Logger
	isClosed   bool
	err        error
	closeOnce  sync.Once
}

// BlockMarshallingError is a wrapper for clickhouse-go/v2 *proto.BlockError
// *proto.BlockError occurs if the driver failed to build clickhouse native proto block.
// Usually it happens due to incorrect input types or values
type BlockMarshallingError struct {
	err  *proto.BlockError
	code coded.Code
}

func (e BlockMarshallingError) Error() string {
	return e.err.Error()
}
func (e BlockMarshallingError) Unwrap() error {
	return e.err
}
func (e BlockMarshallingError) IsMarshallingError() {}
func (e BlockMarshallingError) Code() coded.Code {
	return e.code
}

func (c *chV2Streamer) Append(row abstract.ChangeItem) error {
	var err error
	if err = c.checkClosed(); err != nil {
		return err
	}
	vals, err := c.marshaller(row)
	if err != nil {
		return xerrors.Errorf("error marshalling row for CH: %w", err)
	}
	if err = c.batch.Append(vals...); err != nil {
		c.lgr.Error("Error appending row to batch", log.Error(err))
		c.lgr.Errorf("ChangeItem is %v", row.AsMap())
		c.lgr.Errorf("Values is %v", vals)
		var blockErr *proto.BlockError
		if xerrors.As(err, &blockErr) {
			var code coded.Code
			switch blockErr.Err.(type) {
			case *column.ColumnConverterError:
				code = providers.UnsupportedConversion
			case *column.UnsupportedColumnTypeError:
				code = providers.UnsupportedConversion
			default:
			}
			err = BlockMarshallingError{blockErr, code}
		}

		return xerrors.Errorf("error appending row: %w", err)
	}
	c.memSize += row.Size.Read
	c.batchSize += row.Size.Read
	if c.batchSize > batchSizeLimit {
		if err = c.restart(); err != nil {
			return xerrors.Errorf("error restarting streamer on batch size limit: %w", err)
		}
	} else if c.memSize > memSizeLimit {
		if err = c.flush(); err != nil {
			return xerrors.Errorf("error flushing streaming batch: %w", err)
		}
	}
	return nil
}

func (c *chV2Streamer) Close() error {
	var errs error
	c.closeOnce.Do(func() {
		c.lgr.Info("Closing streaming batch", log.Error(c.err))
		c.isClosed = true
		if !c.batch.IsSent() {
			c.lgr.Debug("Batch is not sent yet, aborting")
			if err := c.batch.Abort(); err != nil {
				errs = multierr.Append(errs, xerrors.Errorf("error aborting CH streaming batch: %w", err))
			}
		}
		c.batch = nil
		if err := c.conn.Close(); err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("error closing CH streaming connection: %w", err))
		}
	})
	return errs
}

// Finish commits all awaiting data and closes Streamer.
func (c *chV2Streamer) Finish() error {
	c.lgr.Infof("Commiting streaming batch")
	if err := c.checkClosed(); err != nil {
		return err
	}
	if err := c.closeIfErr(c.batch.Send); err != nil {
		return xerrors.Errorf("error sending CH streaming batch: %w", err)
	}
	c.lgr.Debug("chV2Streamer closing itself after commit")
	if err := c.Close(); err != nil {
		c.lgr.Warn("error closing streamer", log.Error(err))
	}
	return nil
}

// checkClosed returns nil when chV2Streamer is open.
func (c *chV2Streamer) checkClosed() error {
	if !c.isClosed {
		return nil
	}
	if c.err != nil {
		return xerrors.Errorf("streamer is closed due to error: %w", c.err)
	}
	return xerrors.New("streamer is closed")
}

func (c *chV2Streamer) closeIfErr(fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}
	c.err = err
	c.lgr.Debugf("chV2Streamer closing itself because of error %v", err)
	if closeErr := c.Close(); closeErr != nil {
		c.lgr.Warn("error closing streamer", log.Error(closeErr))
	}
	return err
}

func (c *chV2Streamer) flush() error {
	c.lgr.Debug("Flushing streamer")
	err := c.closeIfErr(c.batch.Flush)
	c.memSize = 0
	return err
}

func (c *chV2Streamer) restart() error {
	c.lgr.Debug("Restarting streamer")
	return c.closeIfErr(func() error {
		if err := c.batch.Send(); err != nil {
			return xerrors.Errorf("error sending streaming batch: %w", err)
		}
		b, err := c.conn.PrepareBatch(context.Background(), c.query)
		if err != nil {
			return xerrors.Errorf("error preparing streaming batch: %w", err)
		}
		c.batch = b
		c.memSize = 0
		c.batchSize = 0
		return nil
	})
}

func newCHV2Streamer(opts *clickhouse.Options, query string, marshaller db.ChangeItemMarshaller, lgr log.Logger) (db.Streamer, error) {
	lgr.Info("Preparing new streaming batch", log.String("query", query))
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, xerrors.Errorf("error getting native Clickhouse connection: %w", err)
	}
	batch, err := conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return nil, xerrors.Errorf("error preparing streaming batch: %w", err)
	}
	return &chV2Streamer{
		conn:       conn,
		batch:      batch,
		query:      query,
		memSize:    0,
		batchSize:  0,
		marshaller: marshaller,
		lgr:        lgr,
		isClosed:   false,
		err:        nil,
		closeOnce:  sync.Once{},
	}, nil
}
