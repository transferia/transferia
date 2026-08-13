package topicwriter

import (
	"bytes"
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	"github.com/transferia/transferia/pkg/util"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	ydb_topicwriter "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type ydbWriter struct {
	conn   *ydb_go_sdk.Driver
	writer *ydb_topicwriter.Writer
}

// async buffered write, don't wait for all data to be successfully saved
func (w *ydbWriter) Write(ctx context.Context, data []byte) error {
	return w.writer.Write(ctx, ydb_topicwriter.Message{Data: bytes.NewReader(data)})
}

func (w *ydbWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err1 := w.writer.Flush(ctx)

	err3 := w.writer.Close(ctx)
	err2 := w.conn.Close(ctx)

	return multierr.Combine(err1, err2, err3)
}

func newYDBWriter(logger log.Logger, cfg *Config) (*ydbWriter, error) {
	var isSecure bool
	opts := []ydb_go_sdk.Option{
		logadapter.WithTraces(logger, trace.RetryEvents),
		ydb_go_sdk.WithDialTimeout(15 * time.Second),
	}
	if creds := cfg.Creds(); creds != nil {
		opts = append(opts, ydb_go_sdk.WithCredentials(creds))
	}
	if cfg.TlsConfig != nil {
		isSecure = true
		opts = append(opts, ydb_go_sdk.WithTLSConfig(cfg.TlsConfig))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := sugar.DSN(cfg.Endpoint(), cfg.DB(), sugar.WithSecure(isSecure))
	conn, err := ydb_go_sdk.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to logbroker: %w", err)
	}
	rollback := util.Rollbacks{}
	defer rollback.Do()
	rollback.Add(func() {
		if err := conn.Close(context.Background()); err != nil {
			logger.Warn("failed to close db conn on rollback", log.Error(err))
		}
	})

	writerOpts := []topicoptions.WriterOption{
		topicoptions.WithWriterProducerID(cfg.SourceID),
		topicoptions.WithWriterCodec(cfg.Codec),
		topicoptions.WithWriterMaxQueueLen(maxWriterQueueLen),
		topicoptions.WithWriterErrOnQueueFull(cfg.WithWriterErrOnQueueFull),
	}

	// Start the writer
	wr, err := conn.Topic().StartWriter(cfg.Topic, writerOpts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create topic writer: %w", err)
	}
	rollback.Add(func() {
		if err := wr.Close(ctx); err != nil {
			logger.Warn("failed to close topic writer on rollback", log.Error(err))
		}
	})

	if err := wr.WaitInit(ctx); err != nil {
		return nil, xerrors.Errorf("failed to wait for topic writer initialization: %w", err)
	}

	rollback.Cancel()

	return &ydbWriter{
		conn:   conn,
		writer: wr,
	}, nil
}
