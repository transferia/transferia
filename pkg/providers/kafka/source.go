//go:build !disable_kafka_provider

package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/functions"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/queues/sequencer"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.ytsaurus.tech/library/go/core/log"
)

var noDataErr = xerrors.NewSentinel("no data")

type reader interface {
	CommitMessages(ctx context.Context, msgs ...kgo.Record) error
	FetchMessage(ctx context.Context) (kgo.Record, error)
	Close() error
}

type Source struct {
	config        *KafkaSource
	metrics       *stats.SourceStats
	logger        log.Logger
	reader        reader
	cancel        context.CancelFunc
	ctx           context.Context
	once          sync.Once
	executor      *functions.Executor
	errCh         chan error
	parser        parsers.Parser
	inflightMutex sync.Mutex
	inflightBytes int
	sequencer     *sequencer.Sequencer

	pmx               sync.Mutex
	partitionReleased bool // becomes true, when consumer loses partitions
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

func (p *Source) waitLimits() {
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for !p.inLimits() {
		time.Sleep(time.Millisecond * 10)
		if p.ctx.Err() != nil {
			p.logger.Warn("context aborted, stop wait for limits")
			return
		}
		if time.Since(logTime) > nextLogDuration {
			logTime = time.Now()
			nextLogDuration = backoffTimer.NextBackOff()
			p.logger.Warnf(
				"reader throttled for %v, limits: %v / %v",
				backoffTimer.GetElapsedTime(),
				format.SizeInt(p.inflightBytes),
				format.SizeInt(int(p.config.BufferSize)),
			)
		}
	}
}

func (p *Source) Run(sink abstract.AsyncSink) error {
	parseWrapper := func(buffer []kgo.Record) []abstract.ChangeItem {
		if len(buffer) == 0 {
			return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}
		}
		return p.parse(buffer)
	}
	parseQ := parsequeue.NewWaitable(p.logger, p.config.ParseQueueParallelism, sink, parseWrapper, p.ack)
	defer parseQ.Close()

	return p.run(parseQ)
}

func (p *Source) run(parseQ *parsequeue.WaitableParseQueue[[]kgo.Record]) error {
	defer func() {
		p.metrics.Master.Set(0)
		p.Stop()
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
		p.metrics.Master.Set(1)
		p.waitLimits()
		select {
		case <-p.ctx.Done():
			return nil
		case err := <-p.errCh:
			p.cancel() // after first error cancel ctx, so any other errors would be dropped, but not deadlocked
			return err
		default:
		}

		fetchCtx, cancel := context.WithTimeout(p.ctx, nextFetchDuration)
		m, err := p.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil && err != errNoInput {
			return xerrors.Errorf("unable to fetch message: %w", err)
		}
		if err == errNoInput && len(buffer) == 0 && len(m.Value) == 0 {
			nextFetchDuration = backoffTimer.NextBackOff()
			p.logger.Info("no input from kafka")
			continue
		}
		backoffTimer.Reset()
		if len(m.Value) != 0 {
			p.addInflight(len(m.Value))
			p.logger.Debugf("read message: %v:%v:%v", m.Topic, m.Partition, m.Offset)
			buffer = append(buffer, m)
			bufferSizeDelta := len(m.Value)
			bufferSize += bufferSizeDelta
			p.metrics.Size.Add(int64(bufferSizeDelta))
			p.metrics.Count.Inc()
		}
		if p.config.Transformer != nil {
			if time.Since(lastPush).Nanoseconds() < p.config.Transformer.BufferFlushInterval.Nanoseconds() &&
				bufferSize < int(p.config.Transformer.BufferSize) {
				continue
			}
		} else {
			if time.Since(lastPush) < time.Second && p.inLimits() {
				continue
			}
		}
		p.logger.Info(
			fmt.Sprintf("begin to process batch: %v items with %v, time from last batch: %v", len(buffer), format.SizeInt(bufferSize), time.Since(lastPush)),
			log.String("offsets", sequencer.BuildMapPartitionToOffsetsRange(recordsToQueueMessages(buffer))),
		)
		err = p.sequencer.StartProcessing(recordsToQueueMessages(buffer))
		if err != nil {
			return xerrors.Errorf("sequencer found an error in StartProcessing, err: %w", err)
		}
		if err := parseQ.Add(buffer); err != nil {
			return xerrors.Errorf("unable to add to pusher q: %w", err)
		}
		lastPush = time.Now()
		buffer = make([]kgo.Record, 0)
		bufferSize = 0

		if p.partitionReleased {
			if err := p.sendSynchronizeEventIfNeeded(parseQ); err != nil {
				return xerrors.Errorf("unable to process partitions loss: %w", err)
			}
		}
	}
}

func (p *Source) Stop() {
	p.once.Do(func() {
		p.cancel()
		if err := p.reader.Close(); err != nil {
			p.logger.Warn("unable to close reader", log.Error(err))
		}
	})
}

func (p *Source) inLimits() bool {
	p.inflightMutex.Lock()
	defer p.inflightMutex.Unlock()
	return p.config.BufferSize == 0 || int(p.config.BufferSize) > p.inflightBytes
}

func (p *Source) addInflight(size int) {
	p.inflightMutex.Lock()
	defer p.inflightMutex.Unlock()
	p.inflightBytes += size
}

func (p *Source) reduceInflight(size int) {
	p.inflightMutex.Lock()
	defer p.inflightMutex.Unlock()
	p.inflightBytes = p.inflightBytes - size
}

func (p *Source) Fetch() ([]abstract.ChangeItem, error) {
	waitTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	var res []abstract.ChangeItem
	var buffer []kgo.Record
	defer p.reader.Close()
	for {
		m, err := p.reader.FetchMessage(ctx)
		if err == nil {
			buffer = append(buffer, m)
		}
		if err == errNoInput || len(buffer) > 2 {
			var data []abstract.ChangeItem
			for _, item := range buffer {
				data = append(data, p.makeRawChangeItem(item))
				res = data
			}
			if p.executor != nil {
				res = nil // reset result
				transformed, err := p.executor.Do(data)
				if err != nil {
					return nil, xerrors.Errorf("failed to execute a batch of changeitems: %w", err)
				}
				data = transformed
				res = transformed
			}
			if p.parser != nil {
				// DO CONVERT
				var parsed []abstract.ChangeItem
				for _, row := range data {
					ci, part := p.changeItemAsMessage(row)
					parsed = append(parsed, p.parser.Do(ci, part)...)
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

func (p *Source) ack(data []kgo.Record, pushSt time.Time, err error) {
	totalSize := 0
	for _, msg := range data {
		totalSize += len(msg.Value)
	}
	defer p.reduceInflight(totalSize)
	if err != nil {
		util.Send(p.ctx, p.errCh, err)
		return
	}
	commitMessages, err := p.sequencer.Pushed(recordsToQueueMessages(data))
	if err != nil {
		util.Send(p.ctx, p.errCh, xerrors.Errorf("sequencer found an error in Pushed, err: %w", err))
		return
	}
	if err := p.reader.CommitMessages(p.ctx, recordsFromQueueMessages(commitMessages)...); err != nil {
		util.Send(p.ctx, p.errCh, err)
		return
	}
	p.logger.Info(
		fmt.Sprintf("Commit messages done in %v", time.Since(pushSt)),
		log.String("pushed", sequencer.BuildMapPartitionToOffsetsRange(recordsToQueueMessages(data))),
		log.String("committed", sequencer.BuildPartitionOffsetLogLine(commitMessages)),
	)
	p.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (p *Source) parse(buffer []kgo.Record) []abstract.ChangeItem {
	var data []abstract.ChangeItem
	totalSize := 0
	for _, msg := range buffer {
		totalSize += len(msg.Value)
		data = append(data, p.makeRawChangeItem(msg))
		msg.Value = nil
	}
	p.logger.Infof("begin transform for batches %v, total size: %v", len(data), format.SizeInt(totalSize))
	if p.executor != nil {
		// DO TRANSFORM
		st := time.Now()
		transformed, err := p.executor.Do(data)
		if err != nil {
			p.logger.Errorf("Cloud function transformation error in %v, %v rows -> %v rows, err: %v", time.Since(st), len(data), len(transformed), err)
			util.Send(p.ctx, p.errCh, xerrors.Errorf("unable to transform message: %w", err))
			return nil
		}
		p.logger.Infof("Cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
		data = transformed
		p.metrics.TransformTime.RecordDuration(time.Since(st))
	}
	if p.parser != nil {
		// DO CONVERT
		st := time.Now()
		var converted []abstract.ChangeItem
		for _, row := range data {
			ci, part := p.changeItemAsMessage(row)
			converted = append(converted, p.parser.Do(ci, part)...)
		}
		p.logger.Infof("convert done in %v, %v rows -> %v rows", time.Since(st), len(data), len(converted))
		data = converted
		p.metrics.DecodeTime.RecordDuration(time.Since(st))
	}
	p.metrics.ChangeItems.Add(int64(len(data)))
	for _, ci := range data {
		if ci.IsRowEvent() {
			p.metrics.Parsed.Inc()
		}
	}
	return data
}

func (p *Source) makeRawChangeItem(msg kgo.Record) abstract.ChangeItem {
	if p.config.IsHomo {
		return MakeKafkaRawMessage(
			msg.Topic,
			msg.Timestamp,
			msg.Topic,
			int(msg.Partition),
			msg.Offset,
			msg.Key,
			msg.Value,
		)
	}

	return abstract.MakeRawMessage(
		msg.Topic,
		msg.Timestamp,
		msg.Topic,
		int(msg.Partition),
		msg.Offset,
		msg.Value,
	)
}

func (p *Source) changeItemAsMessage(ci abstract.ChangeItem) (parsers.Message, abstract.Partition) {
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
			Partition: partition,
			Topic:     ci.Table,
		}
}

func (p *Source) sendSynchronizeEventIfNeeded(parseQ *parsequeue.WaitableParseQueue[[]kgo.Record]) error {
	if p.config.SynchronizeIsNeeded {
		p.logger.Info("Sending synchronize event")
		if err := parseQ.Add([]kgo.Record{}); err != nil {
			return xerrors.Errorf("unable to add message to parser process: %w", err)
		}
		parseQ.Wait()
		p.logger.Info("Sent synchronize event")
	}
	p.partitionReleased = false
	return nil
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

func ensureTopicExists(cl *kgo.Client, topics []string) error {
	req := kmsg.NewMetadataRequest()
	for _, topic := range topics {
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return xerrors.Errorf("unable to check topics existence: %w", err)
	}
	missedTopics := make([]string, 0)
	for _, t := range resp.Topics {
		if t.ErrorCode != kerr.UnknownTopicOrPartition.Code {
			continue
		}
		// despite topic error we still got some partitions
		if len(t.Partitions) > 0 {
			continue
		}

		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		missedTopics = append(missedTopics, name)
	}
	if len(missedTopics) != 0 {
		return coded.Errorf(providers.MissingData, "%v not found, response: %v", missedTopics, resp.Topics)
	}

	return nil
}

func newSource(cfg *KafkaSource, logger log.Logger, registry metrics.Registry) (*Source, error) {
	ctx, cancel := context.WithCancel(context.Background())

	source := &Source{
		config:            cfg,
		metrics:           stats.NewSourceStats(registry),
		reader:            nil,
		logger:            logger,
		cancel:            cancel,
		ctx:               ctx,
		once:              sync.Once{},
		executor:          nil,
		errCh:             make(chan error, 1),
		parser:            nil,
		inflightMutex:     sync.Mutex{},
		inflightBytes:     0,
		sequencer:         sequencer.NewSequencer(),
		pmx:               sync.Mutex{},
		partitionReleased: false,
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

func newSourceWithReader(cfg *KafkaSource, logger log.Logger, registry metrics.Registry, r reader) (*Source, error) {
	source, err := newSource(cfg, logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to create Source: %w", err)
	}
	source.reader = r

	return source, nil
}

func newSourceWithCallbacks(cfg *KafkaSource, logger log.Logger, registry metrics.Registry, opts []kgo.Opt) (*Source, error) {
	source, err := newSource(cfg, logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to create Source: %w", err)
	}

	var topics []string
	if len(cfg.GroupTopics) > 0 {
		topics = cfg.GroupTopics
	} else if cfg.Topic != "" {
		topics = []string{cfg.Topic}
	}
	if len(topics) == 0 {
		return nil, abstract.NewFatalError(xerrors.New("kafka topic required"))
	}

	opts = append(opts,
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, m map[string][]int32) {
			source.pmx.Lock()
			defer source.pmx.Unlock()
			source.partitionReleased = true
		}),
		kgo.ConsumeTopics(topics...),
	)
	if cfg.OffsetPolicy == AtStartOffsetPolicy {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else if cfg.OffsetPolicy == AtEndOffsetPolicy {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	kfClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create kafka client: %w", err)
	}

	if err := backoff.Retry(func() error {
		return ensureTopicExists(kfClient, topics)
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to ensure topic exists: %w", err))
	}

	r := newFranzReader(kfClient)
	source.reader = r

	return source, nil
}

func NewSource(transferID string, cfg *KafkaSource, logger log.Logger, registry metrics.Registry) (*Source, error) {
	tlsConfig, err := cfg.Connection.TLSConfig()
	if err != nil {
		return nil, xerrors.Errorf("unable to get TLS config: %w", err)
	}
	cfg.Auth.Password, err = ResolvePassword(cfg.Connection, cfg.Auth)
	if err != nil {
		return nil, xerrors.Errorf("unable to get password: %w", err)
	}
	mechanism := cfg.Auth.GetFranzAuthMechanism()
	brokers, err := ResolveBrokers(cfg.Connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.ConsumerGroup(transferID),
		kgo.FetchMaxBytes(10 * 1024 * 1024), // 10MB
		kgo.ConnIdleTimeout(30 * time.Second),
		kgo.RequestTimeoutOverhead(20 * time.Second),
		kgo.DisableAutoCommit(),
	}

	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}

	if cfg.BufferSize == 0 {
		cfg.BufferSize = 100 * 1024 * 1024
	}

	return newSourceWithCallbacks(cfg, logger, registry, opts)
}
