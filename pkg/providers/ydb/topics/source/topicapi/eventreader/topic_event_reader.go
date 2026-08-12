package eventreader

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	ErrClosedReader = xerrors.New("topic event reader is closed")
)

const ydbSchemeErrorCode = 400070

type TopicEventReader struct {
	eventsCh <-chan eventWithError
	listener *topiclistener.TopicListener

	stopCh   chan struct{}
	stopOnce sync.Once
}

func (r *TopicEventReader) NextEvent(ctx context.Context) (event.Event, error) {
	select {
	case <-r.stopCh:
		return nil, ErrClosedReader
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, ErrClosedReader
	case e := <-r.eventsCh:
		if e.err != nil {
			return nil, xerrors.Errorf("event handling error: %w", e.err)
		}
		return e.event, nil
	}
}

func (r *TopicEventReader) Close(ctx context.Context) error {
	var err error
	r.stopOnce.Do(func() {
		close(r.stopCh)
		err = r.listener.Close(ctx)
	})
	if err != nil {
		return xerrors.Errorf("closing listener error: %w", err)
	}

	return nil
}

func newTopicEventReader(consumer string, selectors []topicoptions.ReadSelector, ydbClient *ydb.Driver, logger log.Logger) (*TopicEventReader, error) {
	eventsCh := make(chan eventWithError, 1)
	handler := newEventHandler(eventsCh, logger)

	listener, err := ydbClient.Topic().StartListener(
		consumer,
		handler,
		selectors,
		topicoptions.WithListenerAddDecoder(topictypes.CodecZstd, func(input io.Reader) (io.Reader, error) {
			return zstd.NewReader(input)
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to start listener, err: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := listener.WaitInit(ctx); err != nil {
		if ydbOperationErr := ydb.OperationError(err); ydbOperationErr != nil && ydbOperationErr.Code() == ydbSchemeErrorCode {
			return nil, abstract.NewFatalError(
				coded.Errorf(error_codes.MissingData, "topic path does not exist or you do not have access rights: %w ", ydbOperationErr),
			)
		}

		return nil, xerrors.Errorf("unable to init topic listener: %w", err)
	}

	return &TopicEventReader{
		eventsCh: eventsCh,
		listener: listener,
		stopCh:   make(chan struct{}),
		stopOnce: sync.Once{},
	}, nil
}

func NewTopicEventReader(consumer string, topicPaths []string, ydbClient *ydb.Driver, logger log.Logger) (*TopicEventReader, error) {
	selectors := make([]topicoptions.ReadSelector, 0, len(topicPaths))
	for _, topicPath := range topicPaths {
		selectors = append(selectors, topicoptions.ReadSelector{
			Path: topicPath,
		})
	}

	return newTopicEventReader(consumer, selectors, ydbClient, logger)
}
