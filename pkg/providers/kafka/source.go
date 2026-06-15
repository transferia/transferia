package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/functions"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	kafka_reader "github.com/transferia/transferia/pkg/providers/kafka/reader"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/queues/sequencer"
	"github.com/transferia/transferia/pkg/util/throttler"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	noDataErr = xerrors.NewSentinel("no data")
)

type messageReader interface {
	CommitMessages(ctx context.Context, msgs ...kgo.Record) error
	FetchMessage(ctx context.Context) (kgo.Record, error)
	Close() error
}

type Source struct {
	config            *KafkaSource
	metrics           *stats.SourceStats
	logger            log.Logger
	reader            messageReader
	cancel            context.CancelFunc
	ctx               context.Context
	once              sync.Once
	executor          *functions.Executor
	parser            parsers.Parser
	inflightThrottler throttler.Throttler
	sequencer         *sequencer.Sequencer

	pmx               sync.Mutex
	partitionReleased bool // becomes true, when consumer loses partitions
}

func (s *Source) YSRNamespaceID() string {
	if srParser, ok := s.parser.(*parsers.YSRableParser); ok {
		return srParser.YSRNamespaceID()
	}
	return ""
}

// inflightBytes - increased with every new message, but decreased only after AsyncPush returned something,
//     what is a bit counterintuitively at first sight
// so, inflightBytes - it's not some real buffer size, but size of 'virtual' buffer - which includes messages, sent to AsyncPush
//
// behaviour of this method:
// - if we are not exceeded our limit (configured BufferSize) - returns
// - if we exceeded our limit (configured BufferSize) - this method waits when AsyncPush finished and decrease 'inflightBytes'
//       This function returns only when inflightBytes becomes less than BufferSize
//
// actually this is throttler by consumed memory
// backoff is needed here to not write logs too frequently

func (s *Source) waitLimits(parseQDone <-chan struct{}) {
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for s.inflightThrottler.ExceededLimits() {
		select {
		case <-s.ctx.Done():
			s.logger.Warn("context aborted, stop wait for limits")
			return
		case <-parseQDone:
			s.logger.Warn("parse queue stopped, stop wait for limits")
			return
		default:
		}

		if time.Since(logTime) > nextLogDuration {
			logTime = time.Now()
			nextLogDuration = backoffTimer.NextBackOff()
			s.logger.Warnf(
				"reader throttled for %v, limits: %v / %v",
				backoffTimer.GetElapsedTime(),
				format.SizeUInt64(s.inflightThrottler.InflightBytes()),
				format.SizeInt(int(s.config.BufferSizeOrDefault())),
			)
		}
		time.Sleep(time.Millisecond * 20)
	}
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.NewWaitable(s.logger, s.config.ParseQueueParallelism, sink, s.parseWithSynchronizeEvent, s.ack)

	runErr := s.run(parseQ)

	parseQ.Close()
	return multierr.Combine(runErr, parseQ.Error())
}

func (s *Source) run(parseQ parsequeue.WaitableQueue[[]kgo.Record]) error {
	defer func() {
		s.metrics.Master.Set(0)
		s.Stop()
	}()

	var buffer []kgo.Record
	lastPush := time.Now()
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.InitialInterval = time.Second * 15
	backoffTimer.MaxElapsedTime = 0
	backoffTimer.Reset()
	nextFetchDuration := backoffTimer.NextBackOff()
	bufferSize := 0
	for {
		s.metrics.Master.Set(1)
		s.waitLimits(parseQ.Done())
		select {
		case <-s.ctx.Done():
			return nil
		case <-parseQ.Done():
			return nil
		default:
		}

		fetchCtx, cancel := context.WithTimeout(s.ctx, nextFetchDuration)
		m, err := s.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if !xerrors.Is(err, kafka_reader.ErrNoInput) {
				return xerrors.Errorf("unable to fetch message: %w", err)
			} else if len(buffer) == 0 && len(m.Value) == 0 {
				nextFetchDuration = backoffTimer.NextBackOff()
				s.logger.Info("no input from kafka")
				continue
			}
		}

		backoffTimer.Reset()
		if len(m.Value) != 0 {
			s.inflightThrottler.AddInflight(uint64(len(m.Value)))
			s.logger.Debugf("read message: %v:%v:%v", m.Topic, m.Partition, m.Offset)
			buffer = append(buffer, m)
			bufferSizeDelta := len(m.Value)
			bufferSize += bufferSizeDelta
			s.metrics.Size.Add(int64(bufferSizeDelta))
			s.metrics.Count.Inc()
		}
		if s.config.Transformer != nil {
			if time.Since(lastPush).Nanoseconds() < s.config.Transformer.BufferFlushInterval.Nanoseconds() &&
				bufferSize < int(s.config.Transformer.BufferSize) {
				continue
			}
		} else {
			if time.Since(lastPush) < time.Second && !s.inflightThrottler.ExceededLimits() {
				continue
			}
		}

		s.logger.Info(
			fmt.Sprintf("begin to process batch: %v items with %v, time from last batch: %v", len(buffer), format.SizeInt(bufferSize), time.Since(lastPush)),
			log.String("offsets", sequencer.BuildMapPartitionToOffsetsRange(recordsToQueueMessages(buffer))),
		)
		if err = s.sequencer.StartProcessing(recordsToQueueMessages(buffer)); err != nil {
			return xerrors.Errorf("sequencer found an error in StartProcessing, err: %w", err)
		}
		if err := parseQ.Add(buffer); err != nil {
			return xerrors.Errorf("unable to add to pusher q: %w", err)
		}

		lastPush = time.Now()
		buffer = make([]kgo.Record, 0)
		bufferSize = 0

		if s.partitionReleased {
			if err := s.sendSynchronizeEventIfNeeded(parseQ); err != nil {
				return xerrors.Errorf("unable to process partitions loss: %w", err)
			}
		}
	}
}

func (s *Source) Stop() {
	s.once.Do(func() {
		s.cancel()
		if err := s.reader.Close(); err != nil {
			s.logger.Warn("unable to close reader", log.Error(err))
		}
	})
}

func (s *Source) Fetch() ([]abstract.ChangeItem, error) {
	waitTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	var res []abstract.ChangeItem
	var buffer []kgo.Record
	defer func() { _ = s.reader.Close() }()
	for {
		m, err := s.reader.FetchMessage(ctx)
		if err == nil {
			buffer = append(buffer, m)
		}
		if xerrors.Is(err, kafka_reader.ErrNoInput) || len(buffer) > 2 {
			var data []abstract.ChangeItem
			for _, item := range buffer {
				data = append(data, makeRawChangeItem(item))
				res = data
			}
			if s.executor != nil {
				res = nil // reset result
				transformed, err := s.executor.Do(data)
				if err != nil {
					return nil, xerrors.Errorf("failed to execute a batch of change items: %w", err)
				}
				data = transformed
				res = transformed
			}
			if s.parser != nil {
				// DO CONVERT
				var parsed []abstract.ChangeItem
				for _, row := range data {
					ci, part := changeItemAsMessage(row)
					parsed = append(parsed, s.parser.Do(ci, part)...)
				}
				res = parsed
			}
			if len(res) == 0 {
				return nil, xerrors.Errorf("connection established, but %w in %s", noDataErr, waitTimeout)
			}
			return res, nil
		}
		if err != nil {
			return res, xerrors.Errorf("failed to fetch message: %w", err)
		}
	}
}

func (s *Source) ack(data []kgo.Record, pushSt time.Time) error {
	totalSize := 0
	for _, msg := range data {
		totalSize += len(msg.Value)
	}
	defer s.inflightThrottler.ReduceInflight(uint64(totalSize))

	commitMessages, err := s.sequencer.Pushed(recordsToQueueMessages(data))
	if err != nil {
		return xerrors.Errorf("sequencer found an error in Pushed, err: %w", err)
	}
	if err := s.reader.CommitMessages(s.ctx, recordsFromQueueMessages(commitMessages)...); err != nil {
		return err
	}
	s.logger.Info(
		fmt.Sprintf("Commit messages done in %v", time.Since(pushSt)),
		log.String("pushed", sequencer.BuildMapPartitionToOffsetsRange(recordsToQueueMessages(data))),
		log.String("committed", sequencer.BuildPartitionOffsetLogLine(commitMessages)),
	)
	s.metrics.PushTime.RecordDuration(time.Since(pushSt))

	return nil
}

func (s *Source) parseWithSynchronizeEvent(buffer []kgo.Record) ([]abstract.ChangeItem, error) {
	if len(buffer) == 0 {
		return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}, nil
	}
	return s.parse(buffer)
}

func (s *Source) parse(buffer []kgo.Record) ([]abstract.ChangeItem, error) {
	var data []abstract.ChangeItem
	totalSize := 0
	for _, msg := range buffer {
		totalSize += len(msg.Value)
		data = append(data, makeRawChangeItem(msg))
		msg.Value = nil
	}
	s.logger.Infof("begin transform for batches %v, total size: %v", len(data), format.SizeInt(totalSize))
	if s.executor != nil {
		// DO TRANSFORM
		st := time.Now()
		transformed, err := s.executor.Do(data)
		if err != nil {
			s.logger.Errorf("Cloud function transformation error in %v, %v rows -> %v rows, err: %v", time.Since(st), len(data), len(transformed), err)
			return nil, xerrors.Errorf("unable to transform message: %w", err)
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
			ci, part := changeItemAsMessage(row)
			parsedMessages := s.parser.Do(ci, part)
			// it's a workaround for the case when parser doesn't set LSN
			for i := range parsedMessages {
				if parsedMessages[i].LSN == 0 {
					parsedMessages[i].LSN = row.LSN
				}
			}
			converted = append(converted, parsedMessages...)
		}
		s.logger.Infof("convert done in %v, %v rows -> %v rows", time.Since(st), len(data), len(converted))
		data = converted
		s.metrics.DecodeTime.RecordDuration(time.Since(st))
	}
	s.metrics.ChangeItems.Add(int64(len(data)))
	for _, ci := range data {
		if ci.IsRowEvent() {
			s.metrics.Parsed.Inc()
		}
	}
	return data, nil
}

func (s *Source) sendSynchronizeEventIfNeeded(parseQ parsequeue.WaitableQueue[[]kgo.Record]) error {
	if s.config.SynchronizeIsNeeded {
		s.logger.Info("Sending synchronize event")
		if err := parseQ.Add([]kgo.Record{}); err != nil {
			return xerrors.Errorf("unable to add message to parser process: %w", err)
		}
		parseQ.Wait()
		s.logger.Info("Sent synchronize event")
	}
	s.partitionReleased = false
	return nil
}

func makeRawChangeItem(msg kgo.Record) abstract.ChangeItem {
	return abstract.MakeRawMessage(
		msg.Key,
		msg.Topic,
		msg.Timestamp,
		msg.Topic,
		int(msg.Partition),
		msg.Offset,
		msg.Value,
	)
}

func changeItemAsMessage(ci abstract.ChangeItem) (parsers.Message, abstract.Partition) {
	partition := uint32(ci.ColumnValues[1].(int))
	seqNo := ci.ColumnValues[2].(uint64)
	wTime := ci.ColumnValues[3].(time.Time)
	var data []byte
	switch v := ci.ColumnValues[4].(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		panic(fmt.Sprintf("should never happen, expect string or bytes, receive: %T", ci.ColumnValues[4]))
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
			Partition: partition,
			Topic:     ci.Table,
		}
}

func recordsToQueueMessages(records []kgo.Record) []sequencer.QueueMessage {
	messages := make([]sequencer.QueueMessage, len(records))
	for i := range records {
		messages[i].Topic = records[i].Topic
		messages[i].Partition = int(records[i].Partition)
		messages[i].Offset = records[i].Offset
	}
	return messages
}

func recordsFromQueueMessages(messages []sequencer.QueueMessage) []kgo.Record {
	records := make([]kgo.Record, len(messages))
	for i := range records {
		records[i].Topic = messages[i].Topic
		records[i].Partition = int32(messages[i].Partition)
		records[i].Offset = messages[i].Offset
	}
	return records
}

// newBaseSource creates partially initialized Source without reader
func newBaseSource(cfg *KafkaSource, inflightThrottler throttler.Throttler, logger log.Logger, registry core_metrics.Registry) (*Source, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := cfg.WithConnectionID(); err != nil {
		cancel()
		return nil, xerrors.Errorf("unable to resolve connection: %w", err)
	}

	source := &Source{
		config:            cfg,
		metrics:           stats.NewSourceStats(registry),
		reader:            nil,
		logger:            logger,
		cancel:            cancel,
		ctx:               ctx,
		once:              sync.Once{},
		executor:          nil,
		parser:            nil,
		inflightThrottler: inflightThrottler,
		sequencer:         sequencer.NewSequencer(),
		pmx:               sync.Mutex{},
		partitionReleased: false,
	}

	if cfg.Transformer != nil {
		executor, err := functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger)
		if err != nil {
			logger.Error("init function executor", log.Error(err))
			cancel()
			return nil, xerrors.Errorf("unable to init functions transformer: %w", err)
		}
		source.executor = executor
	}

	if cfg.ParserConfig != nil {
		parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, source.metrics)
		if err != nil {
			cancel()
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}
		source.parser = parser
	}

	return source, nil
}

func NewSource(transferID string, cfg *KafkaSource, logger log.Logger, registry core_metrics.Registry) (*Source, error) {
	opts, err := kafkaClientCommonOptions(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to build options: %w", err)
	}

	if err := checkTopicsExistence(opts, cfg.Topics()); err != nil {
		return nil, xerrors.Errorf("unable to check topic existence: %w", err)
	}

	source, err := newBaseSource(cfg, throttler.NewMemoryThrottler(uint64(cfg.BufferSizeOrDefault())), logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to create Source for group: %w", err)
	}

	topics := cfg.Topics()
	if len(topics) == 0 {
		return nil, abstract.NewFatalError(xerrors.New("kafka topic required"))
	}

	opts = append(opts,
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, m map[string][]int32) {
			source.pmx.Lock()
			defer source.pmx.Unlock()
			source.partitionReleased = true
		}),
	)

	if cfg.OffsetPolicy == AtStartOffsetPolicy {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else if cfg.OffsetPolicy == AtEndOffsetPolicy {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	r, err := kafka_reader.NewGroupReader(transferID, topics, opts)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader for group: %w", err)
	}
	source.reader = r

	return source, nil
}
