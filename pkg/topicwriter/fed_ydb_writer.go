package topicwriter

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	ydbfed "github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk"
	"github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtopicwriter"
	"github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtrace"
	"github.com/transferia/transferia/pkg/util"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type fedYDBWriter struct {
	conn   *ydbfed.FedDriver
	writer *fedtopicwriter.FedWriter
}

func (w *fedYDBWriter) Write(ctx context.Context, data []byte) error {
	return w.writer.Write(ctx, fedtopicwriter.NewMessageFromBytes(data))
}

func (w *fedYDBWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err1 := w.writer.Close(ctx)
	err2 := w.conn.Close(ctx)

	return multierr.Combine(err1, err2)
}

func newFedYDBWriter(logger log.Logger, cfg *Config) (*fedYDBWriter, error) {
	var isSecure bool
	opts := []ydbfed.Option{
		ydbfed.WithLogger(logger, fedtrace.ClusterConnection, trace.RetryEvents),
		ydbfed.WithYDBOptions(ydb.WithDialTimeout(15 * time.Second)),
	}
	if creds := cfg.Creds(); creds != nil {
		opts = append(opts, ydbfed.WithCredentials(creds))
	}
	if cfg.TlsConfig != nil {
		isSecure = true
		opts = append(opts, ydbfed.WithYDBOptions(ydb.WithTLSConfig(cfg.TlsConfig)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn := sugar.DSN(cfg.Endpoint(), cfg.DB(), sugar.WithSecure(isSecure))
	fedConn, err := ydbfed.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to logbroker federation: %w", err)
	}
	rollback := util.Rollbacks{}
	defer rollback.Do()
	rollback.Add(func() {
		if err := fedConn.Close(context.Background()); err != nil {
			logger.Warn("failed to close federation connection on rollback", log.Error(err))
		}
	})

	writerOpts := []fedtopicwriter.FedWriterOption{
		fedtopicwriter.WithProducerID(cfg.SourceID),
		fedtopicwriter.WithYdbWriterOptions(
			topicoptions.WithWriterCodec(cfg.Codec),
			topicoptions.WithWriterMaxQueueLen(maxWriterQueueLen),
			topicoptions.WithWriterErrOnQueueFull(cfg.WithWriterErrOnQueueFull),
		),
	}

	// Start the writer
	wr, err := fedConn.StartWriter(ctx, cfg.Topic, writerOpts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to start fed writer for topic %s: %w", cfg.Topic, err)
	}

	rollback.Cancel()

	return &fedYDBWriter{
		conn:   fedConn,
		writer: wr,
	}, nil
}
