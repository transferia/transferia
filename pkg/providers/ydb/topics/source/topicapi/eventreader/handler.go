package eventreader

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	onStartPartitionCallbackName = "OnStartPartitionSessionRequest"
	onReadMessagesCallbackName   = "OnReadMessages"
	onStopPartitionCallbackName  = "OnStopPartitionSessionRequest"
)

type eventWithError struct {
	event event.Event
	err   error
}

type customEventHandler struct {
	topiclistener.BaseHandler

	eventsCh chan<- eventWithError

	logger log.Logger
}

func (h *customEventHandler) OnStartPartitionSessionRequest(ctx context.Context, listenerEvent *topiclistener.EventStartPartitionSession) error {
	if ctx.Err() != nil {
		h.logErrorIfNotNil(ctx.Err(), onStartPartitionCallbackName)
		return xerrors.Errorf("on start partition context error: %w", ctx.Err())
	}

	select {
	case <-ctx.Done():
		h.logErrorIfNotNil(ctx.Err(), onStartPartitionCallbackName)
		return xerrors.Errorf("on start partition context error: %w", ctx.Err())
	case h.eventsCh <- eventWithError{event.NewStartEvent(listenerEvent), nil}:
		h.logger.Debug("listen start reading partition",
			log.String("topic", listenerEvent.PartitionSession.TopicPath),
			log.Int64("partition", listenerEvent.PartitionSession.PartitionID),
		)
		return nil
	}
}

func (h *customEventHandler) OnReadMessages(ctx context.Context, listenerEvent *topiclistener.ReadMessages) error {
	if ctx.Err() != nil {
		h.logErrorIfNotNil(ctx.Err(), onReadMessagesCallbackName)
		return xerrors.Errorf("on read messages context error: %w", ctx.Err())
	}

	readEvent, err := event.NewReadEvent(listenerEvent)

	select {
	case <-ctx.Done():
		h.logErrorIfNotNil(ctx.Err(), onReadMessagesCallbackName)
		return xerrors.Errorf("on read messages context error: %w", ctx.Err())
	case h.eventsCh <- eventWithError{readEvent, err}:
		h.logger.Debug("listen messages",
			log.String("topic", listenerEvent.PartitionSession.TopicPath),
			log.Int64("partition", listenerEvent.PartitionSession.PartitionID),
			log.Int("message_count", len(listenerEvent.Batch.Messages)),
		)
		return nil
	}
}

func (h *customEventHandler) OnStopPartitionSessionRequest(ctx context.Context, listenerEvent *topiclistener.EventStopPartitionSession) error {
	if ctx.Err() != nil {
		h.logErrorIfNotNil(ctx.Err(), onStopPartitionCallbackName)
		return xerrors.Errorf("on stop partition context error: %w", ctx.Err())
	}

	select {
	case <-ctx.Done():
		h.logErrorIfNotNil(ctx.Err(), onStopPartitionCallbackName)
		return xerrors.Errorf("on stop partition context error: %w", ctx.Err())
	case h.eventsCh <- eventWithError{event.NewStopEvent(listenerEvent), nil}:
		h.logger.Debug("listen stop reading partition", log.Bool("graceful", listenerEvent.Graceful))
		return nil
	}
}

func (h *customEventHandler) logErrorIfNotNil(err error, callbackName string) {
	if err == nil {
		return
	}

	h.logger.Debug("topic listener callback context canceled",
		log.String("callback", callbackName),
		log.Error(err),
	)
}

func newEventHandler(eventsCh chan eventWithError, logger log.Logger) topiclistener.EventHandler {
	return &customEventHandler{
		BaseHandler: topiclistener.BaseHandler{},
		eventsCh:    eventsCh,
		logger:      logger,
	}
}
