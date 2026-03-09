package eventhub

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
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

const (
	receiveBatchSize = 100
	receiveTimeout   = 2 * time.Second
	flushPollPeriod  = 50 * time.Millisecond
	closeTimeout     = 10 * time.Second
)

type receivedEvent struct {
	partitionID string
	event       *azeventhubs.ReceivedEventData
}

type Source struct {
	ctx                            context.Context
	nameSpace, hubName, transferID string
	logger                         log.Logger
	cancelFunc                     context.CancelFunc
	once                           sync.Once
	startPosition                  azeventhubs.StartPosition
	stopCh                         chan struct{}
	errCh                          chan error
	dataCh                         chan receivedEvent
	metrics                        *stats.SourceStats
	transformer                    *model.DataTransformOptions
	executor                       *functions.Executor
	parser                         parsers.Parser
	consumerClient                 *azeventhubs.ConsumerClient
	partitionClients               map[string]*azeventhubs.PartitionClient
	partitionClientsMu             sync.Mutex
}

func (s *Source) YSRNamespaceID() string {
	if srParser, ok := s.parser.(*parsers.YSRableParser); ok {
		return srParser.YSRNamespaceID()
	}
	return ""
}

func parseStartPosition(cfg *EventHubSource) (azeventhubs.StartPosition, error) {
	if cfg.StartingOffset != "" && cfg.StartingTimeStamp != nil {
		return azeventhubs.StartPosition{}, xerrors.New("cannot use StartingOffset and StartingTimeStamp simultaneously")
	}

	pos := azeventhubs.StartPosition{}

	if cfg.StartingOffset != "" {
		pos.Offset = &cfg.StartingOffset
	}

	if cfg.StartingTimeStamp != nil {
		ts := cfg.StartingTimeStamp.UTC()
		pos.EnqueuedTime = &ts
	}

	return pos, nil
}

func (s *Source) readPartition(partitionID string, partitionClient *azeventhubs.PartitionClient) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.stopCh:
			return
		default:
		}

		receiveCtx, cancel := context.WithTimeout(s.ctx, receiveTimeout)
		events, err := partitionClient.ReceiveEvents(receiveCtx, receiveBatchSize, nil)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			select {
			case s.errCh <- fmt.Errorf("failed to receive events from partition %s: %w", partitionID, err):
			case <-s.ctx.Done():
			case <-s.stopCh:
			}
			continue
		}

		for _, event := range events {
			if event == nil || len(event.Body) == 0 {
				continue
			}

			select {
			case s.dataCh <- receivedEvent{partitionID: partitionID, event: event}:
			case <-s.ctx.Done():
				return
			case <-s.stopCh:
				return
			}
		}
	}
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	defer s.Stop()

	info, err := s.consumerClient.GetEventHubProperties(s.ctx, nil)
	if err != nil {
		return fmt.Errorf("fail to get runtime information: %w", err)
	}
	s.logger.Infof("start receiving from hub %s via %d partitions", info.Name, len(info.PartitionIDs))

	s.partitionClientsMu.Lock()
	for _, partitionID := range info.PartitionIDs {
		partitionClient, err := s.consumerClient.NewPartitionClient(partitionID, &azeventhubs.PartitionClientOptions{
			StartPosition: s.startPosition,
		})
		if err != nil {
			s.partitionClientsMu.Unlock()
			return fmt.Errorf("fail to init receiver from partition %s: %w", partitionID, err)
		}
		s.partitionClients[partitionID] = partitionClient
		go s.readPartition(partitionID, partitionClient)
	}
	s.partitionClientsMu.Unlock()

	lastPush := time.Now()
	buffer := make([]receivedEvent, 0)
	bufferSize := 0
	flushTicker := time.NewTicker(flushPollPeriod)
	defer flushTicker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		batch := make([]receivedEvent, len(buffer))
		copy(batch, buffer)
		go s.processMessages(sink, batch)
		lastPush = time.Now()
		buffer = buffer[:0]
		bufferSize = 0
	}

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
			bufferSize += len(msg.event.Body)
		case <-flushTicker.C:
			if len(buffer) == 0 {
				continue
			}
			if s.transformer != nil &&
				time.Since(lastPush).Nanoseconds() < s.transformer.BufferFlushInterval.Nanoseconds() &&
				bufferSize < int(s.transformer.BufferSize) {
				continue
			}
			flush()
		}
	}
}

func (s *Source) makeRawChangeItem(partitionID string, event *azeventhubs.ReceivedEventData) abstract.ChangeItem {
	partitionIDInt := 0
	if p, err := strconv.Atoi(partitionID); err == nil {
		partitionIDInt = p
	}

	offset := uint64(event.SequenceNumber)
	if parsedOffset, err := strconv.ParseUint(event.Offset, 10, 64); err == nil {
		offset = parsedOffset
	}

	enqueueTime := time.Now().UTC()
	if event.EnqueuedTime != nil {
		enqueueTime = *event.EnqueuedTime
	}

	topic := fmt.Sprintf("%v_%v", s.transferID, partitionIDInt)

	return abstract.MakeRawMessage(
		[]byte("stub"),
		s.transferID,
		enqueueTime,
		topic,
		partitionIDInt,
		int64(offset),
		event.Body,
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
			Partition: uint32(partition),
			Topic:     ci.Schema,
		}
}

func (s *Source) processMessages(sink abstract.AsyncSink, buffer []receivedEvent) {
	var data []abstract.ChangeItem
	totalSize := 0
	for _, item := range buffer {
		totalSize += len(item.event.Body)
		data = append(data, s.makeRawChangeItem(item.partitionID, item.event))
	}

	s.logger.Infof("begin transform for batches %v, total size: %v", len(data), format.SizeInt(totalSize))

	if s.executor != nil {
		st := time.Now()
		transformed, err := s.executor.Do(data)
		if err != nil {
			s.logger.Errorf(
				"Cloud function transformation error in %v, %v rows -> %v rows, err: %v",
				time.Since(st),
				len(data),
				len(transformed),
				err,
			)
			select {
			case s.errCh <- xerrors.Errorf("unable to transform message: %w", err):
			default:
			}
			return
		}
		s.logger.Infof("Cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
		data = transformed
		s.metrics.TransformTime.RecordDuration(time.Since(st))
	}

	if s.parser != nil {
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
		select {
		case s.errCh <- fmt.Errorf("failed to push items: %w", err):
		default:
		}
	} else {
		s.metrics.PushTime.RecordDuration(time.Since(pushStart))
	}
}

func (s *Source) Stop() {
	s.once.Do(func() {
		close(s.stopCh)
		s.cancelFunc()

		closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		s.partitionClientsMu.Lock()
		for partitionID, partitionClient := range s.partitionClients {
			if partitionClient == nil {
				continue
			}
			if err := partitionClient.Close(closeCtx); err != nil {
				s.logger.Warn(fmt.Sprintf("failed to close receiver for %s partition", partitionID), log.Error(err))
			}
		}
		s.partitionClientsMu.Unlock()

		if s.consumerClient != nil {
			if err := s.consumerClient.Close(closeCtx); err != nil {
				s.logger.Warn("failed to close eventhub consumer client", log.Error(err))
			}
		}
	})
}

func buildConnectionString(cfg *EventHubSource) string {
	namespace := strings.TrimSpace(cfg.NamespaceName)
	namespace = strings.TrimPrefix(namespace, "sb://")
	namespace = strings.TrimSuffix(namespace, "/")

	if !strings.Contains(namespace, ".") {
		namespace += ".servicebus.windows.net"
	}

	return fmt.Sprintf(
		"Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s",
		namespace,
		cfg.Auth.KeyName,
		string(cfg.Auth.KeyValue),
	)
}

func NewSource(transferID string, cfg *EventHubSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	if cfg.Auth == nil {
		return nil, xerrors.New("eventhub auth is required")
	}

	if cfg.Auth.Method != EventHubAuthSAS {
		return nil, fmt.Errorf("wrong auth method: %s", cfg.Auth.Method)
	}

	startPosition, err := parseStartPosition(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse opts: %w", err)
	}

	consumerGroup := cfg.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = azeventhubs.DefaultConsumerGroup
	}

	connString := buildConnectionString(cfg)
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(
		connString,
		cfg.HubName,
		consumerGroup,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init eventhub consumer client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	source := &Source{
		ctx:                ctx,
		consumerClient:     consumerClient,
		metrics:            stats.NewSourceStats(registry),
		startPosition:      startPosition,
		transformer:        cfg.Transformer,
		nameSpace:          cfg.NamespaceName,
		hubName:            cfg.HubName,
		transferID:         transferID,
		logger:             logger,
		cancelFunc:         cancel,
		once:               sync.Once{},
		stopCh:             make(chan struct{}),
		errCh:              make(chan error, 128),
		dataCh:             make(chan receivedEvent, 1024),
		executor:           nil,
		parser:             nil,
		partitionClientsMu: sync.Mutex{},
		partitionClients:   make(map[string]*azeventhubs.PartitionClient),
	}

	if cfg.Transformer != nil {
		executor, err := functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger, registry)
		if err != nil {
			logger.Error("init function executor", log.Error(err))
			_ = source.consumerClient.Close(context.Background())
			return nil, xerrors.Errorf("unable to init functions transformer: %w", err)
		}
		source.executor = executor
	}

	if cfg.ParserConfig != nil {
		parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, source.metrics)
		if err != nil {
			_ = source.consumerClient.Close(context.Background())
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}
		source.parser = parser
	}

	return source, nil
}
