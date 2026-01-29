package ydb

import (
	"context"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

const sourceBatchMaxLen = 10000

type readerThreadSafe struct {
	// RWMutex is used to make Close() mutually exclusive with any in-flight reader operations.
	//
	// We allow ReadMessageBatch()/Commit() under RLock: they may run in parallel with each other,
	// and the ydb-go-sdk/topicreader implementation is responsible for synchronizing read/commit semantics.
	// but Close() must acquire the write lock to ensure the underlying topic reader isn't closed
	// while a read/commit RPC is still using it.
	mutex      sync.RWMutex
	readerImpl *topicreader.Reader
}

func (r *readerThreadSafe) ReadMessageBatch(ctx context.Context) (*topicreader.Batch, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.readerImpl.ReadMessagesBatch(ctx)
}

func (r *readerThreadSafe) Commit(ctx context.Context, batch *topicreader.Batch) error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.readerImpl.Commit(ctx, batch)
}

func (r *readerThreadSafe) Close(ctx context.Context) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.readerImpl.Close(ctx)
}

func newReader(feedName, consumerName, dbname string, tables []string, ydbClient *ydb.Driver, commitMode topicoptions.CommitMode, logger log.Logger) (*readerThreadSafe, error) {
	dbname = strings.TrimLeft(dbname, "/")
	selectors := make([]topicoptions.ReadSelector, len(tables))
	for i, table := range tables {
		table = strings.TrimLeft(table, "/")
		selectors[i] = topicoptions.ReadSelector{
			Path: makeChangeFeedPath(path.Join(dbname, table), feedName),
		}
	}

	readerImpl, err := ydbClient.Topic().StartReader(
		consumerName,
		selectors,
		topicoptions.WithReaderCommitTimeLagTrigger(0),
		topicoptions.WithReaderCommitMode(commitMode),
		topicoptions.WithReaderBatchMaxCount(sourceBatchMaxLen),
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderError: func(info trace.TopicReaderErrorInfo) {
				if xerrors.Is(info.Error, io.EOF) {
					logger.Warnf("topic reader received %s and will reconnect", info.Error)
					return
				}
				logger.Errorf("topic reader error: %s", info.Error)
			},
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to start reader, err: %w", err)
	}

	return &readerThreadSafe{
		mutex:      sync.RWMutex{},
		readerImpl: readerImpl,
	}, nil
}
