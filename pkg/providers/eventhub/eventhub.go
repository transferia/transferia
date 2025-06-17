//go:build !disable_eventhub_provider

package eventhub

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/sas"
	eventhubs "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/functions"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type Source struct {
	ctx                            context.Context
	nameSpace, hubName, transferID string
	logger                         log.Logger
	cancelFunc                     context.CancelFunc
	once                           sync.Once
	receiveOpts                    []eventhubs.ReceiveOption
	handlerRegistry                map[string]*eventhubs.ListenerHandle
	errCh                          chan error
	dataCh                         chan *eventhubs.Event
	stopCh                         chan struct{}
	hub                            *eventhubs.Hub
	metrics                        *stats.SourceStats
	transformer                    *model.DataTransformOptions
	executor                       *functions.Executor
	parser                         parsers.Parser
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	defer func() {
		s.Stop()
	}()
	lastPush := time.Now()
	var buffer []*eventhubs.Event
	bufferSize := 0

	info, err := s.hub.GetRuntimeInformation(s.ctx)
	if err != nil {
		return fmt.Errorf("fail to get runtime information: %w", err)
	}
	s.logger.Infof("start receiving from hub %s via %d partitions", info.Path, info.PartitionCount)

	for _, pID := range info.PartitionIDs {
		partitionID := pID
		handler, err := s.hub.Receive(
			s.ctx,
			partitionID,
			s.getHandlerFunc(s.dataCh),
			s.receiveOpts...,
		)
		if err != nil {
			return fmt.Errorf("fail to init receiver from partition %s: %w", partitionID, err)
		}
		s.handlerRegistry[partitionID] = handler
	}
	s.watchHandlers()

	for {
		select {
		case <-s.stopCh:
			s.logger.Warn("Run was stopped")
			return nil
		case err := <-s.errCh:
			s.logger.Error("error while running", log.Error(err))
		case msg, ok := <-s.dataCh:
			if !ok {
				s.logger.Warn("Receiver was closed")
				return xerrors.New("Receiver was closed, finish running")
			}
			buffer = append(buffer, msg)
			bufferSize += len(msg.Data)
		default:
			if len(buffer) == 0 {
				continue
			}
			if s.transformer != nil {
				if time.Since(lastPush).Nanoseconds() < s.transformer.BufferFlushInterval.Nanoseconds() &&
					bufferSize < int(s.transformer.BufferSize) {
					continue
				}
			}
			go func(buffer []*eventhubs.Event) {
				s.processMessages(sink, buffer)
			}(buffer)
			lastPush = time.Now()
			buffer = make([]*eventhubs.Event, 0)
			bufferSize = 0
		}
	}
}

func parseOpts(cfg *EventHubSource) (opts []eventhubs.ReceiveOption, err error) {
	if cfg.StartingOffset != "" && cfg.StartingTimeStamp != nil {
		return nil, xerrors.New("cannot use StartingOffset and StartingTimeStamp simultaneously")
	}
	if cfg.StartingOffset != "" {
		opts = append(opts, eventhubs.ReceiveWithStartingOffset(cfg.StartingOffset))
	}
	if cfg.StartingTimeStamp != nil {
		opts = append(opts, eventhubs.ReceiveFromTimestamp(*cfg.StartingTimeStamp))
	}
	if cfg.ConsumerGroup != "" {
		opts = append(opts, eventhubs.ReceiveWithConsumerGroup(cfg.ConsumerGroup))
	}

	return opts, nil
}

func (s *Source) getHandlerFunc(dataCh chan *eventhubs.Event) eventhubs.Handler {
	return func(ctx context.Context, event *eventhubs.Event) error {
		if event == nil {
			return nil
		}

		msg := event.Data
		msgSize := len(msg)
		if msgSize == 0 {
			return nil
		}

		dataCh <- event
		return nil
	}
}

func (s *Source) watchHandlers() {
	for pID, h := range s.handlerRegistry {
		if h == nil {
			continue
		}
		handler := h
		partitionID := pID
		go func() {
			select {
			case <-handler.Done():
				listenerErr := handler.Err()
				if listenerErr == nil || xerrors.Is(listenerErr, context.Canceled) {
					s.logger.Infof("listener %s was finished successfully", partitionID)
					return
				}
				s.logger.Warn(fmt.Sprintf("listener %s was finished with err", partitionID), log.Error(listenerErr))
				s.errCh <- listenerErr
			case <-s.ctx.Done():
				return
			}
		}()
	}
}

func (s *Source) makeRawChangeItem(event *eventhubs.Event) abstract.ChangeItem {
	partitionID := 0 // TODO(melkikh): understand, why Event.PartitionKey and Event.SystemProperties.PartitionKey are always nil
	offset := uint64(*event.SystemProperties.Offset)
	if event.SystemProperties.PartitionKey != nil {
		pID, err := strconv.Atoi(*event.SystemProperties.PartitionKey)
		if err == nil {
			partitionID = pID
		}
	} else if event.SystemProperties.PartitionID != nil {
		partitionID = int(*event.SystemProperties.PartitionID)
	}
	topic := fmt.Sprintf("%v_%v", s.transferID, partitionID)
	return abstract.MakeRawMessage(
		s.transferID,
		*event.SystemProperties.EnqueuedTime,
		topic,
		partitionID,
		int64(offset),
		event.Data,
	)
}

func (s *Source) changeItemAsMessage(ci abstract.ChangeItem) (parsers.Message, abstract.Partition) {
	partition := ci.ColumnValues[1].(int)
	seqNo := ci.ColumnValues[2].(uint64)
	wTime := ci.ColumnValues[3].(time.Time)
	var data []byte
	switch v := ci.ColumnValues[4].(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		panic(fmt.Sprintf("should never happen, expect string or bytes, recieve: %T", ci.ColumnValues[4]))
	}
	return parsers.Message{
			Offset:     ci.LSN,
			SeqNo:      seqNo,
			Key:        nil,
			CreateTime: time.Unix(0, int64(ci.CommitTime)),
			WriteTime:  wTime,
			Value:      data,
			Headers:    nil,
		}, abstract.Partition{
			Cluster:   "", // v1 protocol does not contains such entity
			Partition: uint32(partition),
			Topic:     ci.Schema,
		}
}

func (s *Source) processMessages(sink abstract.AsyncSink, buffer []*eventhubs.Event) {
	var data []abstract.ChangeItem
	totalSize := 0
	for _, event := range buffer {
		totalSize += len(event.Data)
		data = append(data, s.makeRawChangeItem(event))
	}
	s.logger.Infof("begin transform for batches %v, total size: %v", len(data), format.SizeInt(totalSize))
	if s.executor != nil {
		// DO TRANSFORM
		st := time.Now()
		transformed, err := s.executor.Do(data)
		if err != nil {
			s.logger.Errorf("Cloud function transformation error in %v, %v rows -> %v rows, err: %v", time.Since(st), len(data), len(transformed), err)
			s.errCh <- xerrors.Errorf("unable to transform message: %w", err)
			return
		}
		s.logger.Infof("Cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
		data = transformed
		s.metrics.TransformTime.RecordDuration(time.Since(st))
	}
	if s.parser != nil {
		// DO CONVERT
		st := time.Now()
		var converted []abstract.ChangeItem
		for _, row := range data {
			ci, part := s.changeItemAsMessage(row)
			converted = append(converted, s.parser.Do(ci, part)...)
		}
		s.logger.Infof("convert done in %v, %v rows -> %v rows", time.Since(st), len(data), len(converted))
		data = converted
		s.metrics.DecodeTime.RecordDuration(time.Since(st))

	}
	pushStart := time.Now()
	if err := <-sink.AsyncPush(data); err != nil {
		s.errCh <- fmt.Errorf("failed to push items: %w", err)
	} else {
		s.metrics.PushTime.RecordDuration(time.Since(pushStart))
	}
}

func (s *Source) Stop() {
	s.once.Do(func() {
		close(s.stopCh)
		for partitionID, h := range s.handlerRegistry {
			if err := h.Close(s.ctx); err != nil {
				s.logger.Warn(fmt.Sprintf("failed to close receiver for %s partition", partitionID), log.Error(err))
			}
		}
		s.cancelFunc()
	})
}

func NewSource(transferID string, cfg *EventHubSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	var hub *eventhubs.Hub
	switch method := cfg.Auth.Method; method {
	case EventHubAuthSAS:
		tokenProvider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(cfg.Auth.KeyName, string(cfg.Auth.KeyValue)))
		if err != nil {
			return nil, fmt.Errorf("failed to init SAS token provider: %w", err)
		}
		hub, err = eventhubs.NewHub(
			cfg.NamespaceName,
			cfg.HubName,
			tokenProvider,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to init eventhub: %w", err)
		}
	default:
		return nil, fmt.Errorf("wrong auth method: %s", method)
	}

	opts, err := parseOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse opts: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	source := &Source{
		ctx:             ctx,
		hub:             hub,
		metrics:         stats.NewSourceStats(registry),
		transformer:     cfg.Transformer,
		nameSpace:       cfg.NamespaceName,
		hubName:         cfg.HubName,
		transferID:      transferID,
		logger:          logger,
		cancelFunc:      cancel,
		once:            sync.Once{},
		receiveOpts:     opts,
		handlerRegistry: make(map[string]*eventhubs.ListenerHandle),
		errCh:           make(chan error),
		dataCh:          make(chan *eventhubs.Event),
		stopCh:          make(chan struct{}),
		executor:        nil,
		parser:          nil,
	}

	if cfg.Transformer != nil {
		executor, err := functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger, registry)
		if err != nil {
			logger.Error("init function executor", log.Error(err))
			return nil, xerrors.Errorf("unable to init functions transformer: %w", err)
		}
		source.executor = executor
	}

	if cfg.ParserConfig != nil {
		parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, source.metrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}
		source.parser = parser
	}

	return source, nil
}
