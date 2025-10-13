package logbroker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/resources"
	"github.com/transferia/transferia/pkg/providers/logbroker/queues"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

type oneDCSource struct {
	config           *LfSource
	parser           parsers.Parser
	offsetsValidator *queues.LbOffsetsSourceValidator
	consumer         persqueue.Reader
	cancel           context.CancelFunc

	onceStop sync.Once
	stopCh   chan bool // No one ever write to this channel (so it's type doesn't matter). Used only as signal when closed

	onceErr sync.Once
	errCh   chan error // unbuffered chan, can recv only one error (first ocurred)

	logger  log.Logger
	metrics *stats.SourceStats

	runningWG sync.WaitGroup // to not write into closed channel when another goroutine closes us
	lastRead  time.Time

	maxBatchSize int
}

func (s *oneDCSource) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		b, ok := <-s.consumer.C()
		if !ok {
			return nil, errors.New("consumer closed, close subscription")
		}
		select {
		case <-ctx.Done():
			return nil, errors.New("context deadline")
		default:
		}
		switch v := b.(type) {
		case *persqueue.Lock:
			s.lockPartition(v)
			continue
		case *persqueue.Release:
			_ = s.sendSynchronizeEventIfNeeded(nil)
			continue
		case *persqueue.Data:
			var res []abstract.ChangeItem
			raw := make([]abstract.ChangeItem, 0)
			parseWrapper := func(buffer []*persqueue.Data) []abstract.ChangeItem {
				for _, item := range buffer {
					for _, b := range item.Batches() {
						for _, m := range b.Messages {
							raw = append(raw, queues.MessageAsChangeItem(parsers.Message{
								Offset:     m.Offset,
								SeqNo:      m.SeqNo,
								Key:        m.SourceID,
								CreateTime: m.CreateTime,
								WriteTime:  m.WriteTime,
								Value:      m.Data,
								Headers:    m.ExtraFields,
							}, parsers.MessageBatch{
								Topic:     b.Topic,
								Partition: b.Partition,
								Messages:  nil, // not used here
							}))
						}
					}
				}
				return queues.Parse(buffer, s.parser, s.metrics, s.logger, nil)
			}
			parsed := parseWrapper([]*persqueue.Data{v})
			if len(raw) > 3 {
				raw = raw[:3]
			}
			if len(parsed) > 3 {
				parsed = parsed[:3]
			}
			res = parsed
			for _, rawChangeItem := range raw {
				rawChangeItem.Schema = "raw"
				res = append(res, rawChangeItem)
			}
			return res, nil
		case *persqueue.Disconnect:
			if v.Err != nil {
				s.logger.Errorf("Disconnected: %s", v.Err.Error())
			} else {
				s.logger.Error("Disconnected")
			}
			_ = s.sendSynchronizeEventIfNeeded(nil)
			continue
		default:
			continue
		}
	}
}

func (s *oneDCSource) Stop() {
	s.onceStop.Do(func() {
		close(s.stopCh)
		s.runningWG.Wait() // it should be before closing 'pushCh' - we are waiting when 'Run' is done
		s.cancel()
		if resourceable, ok := s.parser.(resources.Resourceable); ok {
			resourceable.ResourcesObj().Close()
		}
		s.logger.Infof("cancel reader. Inflight:%d, WaitAck:%d", s.consumer.Stat().InflightCount, s.consumer.Stat().WaitAckCount)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for {
		select {
		case m := <-s.consumer.C():
			s.logger.Infof("Inflight: %v, WaitAck: %v", s.consumer.Stat().InflightCount, s.consumer.Stat().WaitAckCount)
			switch v := m.(type) {
			case *persqueue.CommitAck:
				s.logger.Infof("message ack: %v", v.Cookies)
			case *persqueue.Data:
				skippedMessages := map[string]map[uint32][]uint64{}
				for _, b := range v.Batches() {
					skippedMessages[b.Topic] = map[uint32][]uint64{}
					for _, msg := range b.Messages {
						skippedMessages[b.Topic][b.Partition] = append(skippedMessages[b.Topic][b.Partition], msg.Offset)
					}
				}
				s.logger.Info("skipped message data messages", log.Any("cookie", v.Cookie), log.Any("skipped_messages", skippedMessages))
			case *persqueue.Disconnect:
				if v.Err != nil {
					s.logger.Infof("Disconnected: %s", v.Err.Error())
				} else {
					s.logger.Info("Disconnected")
				}
			}
		case <-ctx.Done():
			s.logger.Error("timeout in lb reader abort")
			return
		case <-s.consumer.Closed():
			s.logger.Info("abort lb reader", log.Any("callstack", util.GetCurrentGoroutineCallstack()))
			return
		}
	}
}

func (s *oneDCSource) Run(sink abstract.AsyncSink) error {
	defer s.Stop()
	defer func() {
		select {
		case <-s.consumer.Closed():
			return
		default:
			s.logger.Info("Start gracefully close lb reader")
			s.Stop()
		}
	}()
	defer s.runningWG.Done() // it should be lower than 'defer s.Stop()' to escape deadlock

	parseWrapper := func(buffer batch) []abstract.ChangeItem {
		if len(buffer.Batches) == 0 {
			return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}
		}
		return s.parse(buffer)
	}
	parseQ := parsequeue.NewWaitable[batch](s.logger, s.config.ParseQueueParallelism, sink, parseWrapper, s.ack)
	defer parseQ.Close()

	return s.run(parseQ)
}

func (s *oneDCSource) run(parseQ *parsequeue.WaitableParseQueue[batch]) error {
	s.runningWG.Add(1)
	timer := time.NewTimer(time.Second)
	sessionID := ""

	for {
		select {
		case <-s.stopCh:
			s.logger.Info("Stop oneDCSource")
			return nil
		case <-timer.C:
			stat := s.consumer.Stat()
			s.logger.Debug(
				"Ticker",
				log.Any("usage", format.SizeInt(stat.MemUsage)),
				log.Any("readed", format.SizeUInt64(stat.BytesRead)),
				log.Any("extracted", format.SizeUInt64(stat.BytesExtracted)),
			)
			s.metrics.Usage.Set(float64(stat.MemUsage))
			s.metrics.Read.Set(float64(stat.BytesRead))
			s.metrics.Extract.Set(float64(stat.BytesExtracted))
			if stat.BytesRead > 0 {
				s.metrics.CompressRatio.Set(float64(stat.BytesExtracted / stat.BytesRead))
			}
			sessionID = stat.SessionID
			timer = time.NewTimer(time.Second)
		case err := <-s.errCh:
			s.logger.Error("consumer error", log.Error(err))
			return err
		case b, ok := <-s.consumer.C():
			if !ok {
				s.logger.Warn("Reader closed")
				return errors.New("consumer closed, close subscription")
			}

			s.metrics.Master.Set(1)
			switch v := b.(type) {
			case *persqueue.CommitAck:
				s.logger.Infof("Ack in %v with %v", sessionID, v.Cookies)
			case *persqueue.Lock:
				s.lockPartition(v)
			case *persqueue.Release:
				s.logger.Infof("Received 'Release' event, partition:%s@%d", v.Topic, v.Partition)
				err := s.sendSynchronizeEventIfNeeded(parseQ)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
			case *persqueue.Disconnect:
				if v.Err != nil {
					s.logger.Errorf("Disconnected: %s", v.Err.Error())
				} else {
					s.logger.Error("Disconnected")
				}
				err := s.sendSynchronizeEventIfNeeded(parseQ)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
			case *persqueue.Data:
				batches := queues.ConvertBatches(v.Batches())
				err := s.offsetsValidator.CheckLbOffsets(batches)
				if err != nil {
					if s.config.AllowTTLRewind {
						s.logger.Warn("ttl rewind", log.Error(err))
					} else {
						s.metrics.Fatal.Inc()
						return abstract.NewFatalError(err)
					}
				}
				s.logger.Debug("got lb_offsets", log.Any("range", queues.BuildMapPartitionToLbOffsetsRange(batches)))

				s.lastRead = time.Now()
				messagesSize, messagesCount := BatchStatistics(batches)
				s.metrics.Size.Add(int64(messagesSize))
				s.metrics.Count.Add(int64(messagesCount))
				s.logger.Debugf("Incoming data: %d messages of total size %d", messagesCount, messagesSize)

				splittedBatches := newBatches(s.maxBatchSize, v.Commit, batches)

				for _, batch := range splittedBatches {
					if err := parseQ.Add(batch); err != nil {
						return xerrors.Errorf("unable to add message to parser process: %w", err)
					}
				}
			}
		}
	}
}

func (s *oneDCSource) WatchResource(resources resources.AbstractResources) {
	select {
	case <-resources.OutdatedCh():
		s.logger.Warn("Parser resource is outdated, stop oneDCSource")
		s.Stop()
	case <-s.stopCh:
		return
	}
}

func (s *oneDCSource) lockPartition(lock *persqueue.Lock) {
	partName := fmt.Sprintf("%v@%v", lock.Topic, lock.Partition)
	s.logger.Infof("Lock partition %v %v - %v", partName, lock.ReadOffset, lock.EndOffset)
	s.offsetsValidator.InitOffsetForPartition(lock.Topic, lock.Partition, lock.ReadOffset)
	lock.StartRead(true, lock.ReadOffset, lock.ReadOffset)
}

func (s *oneDCSource) sendSynchronizeEventIfNeeded(parseQ *parsequeue.WaitableParseQueue[batch]) error {
	if s.config.IsLbSink && parseQ != nil {
		s.logger.Info("Sending synchronize event")
		if err := parseQ.Add(newBatch([]parsers.MessageBatch{})); err != nil {
			return xerrors.Errorf("unable to add message to parser process: %w", err)
		}
		parseQ.Wait()
		s.logger.Info("Sent synchronize event")
	}
	return nil
}

func (s *oneDCSource) monitorIdle(duration time.Duration) {
	ticker := time.NewTicker(time.Minute)
	defer s.Stop()
	for {
		select {
		case <-ticker.C:
			if time.Since(s.lastRead) > duration {
				s.logger.Warn("too long time no any update")
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *oneDCSource) ack(data batch, st time.Time, err error) {
	if err != nil {
		s.onceErr.Do(func() {
			s.errCh <- err
		})
		return
	} else {
		data.Commit()
		s.metrics.PushTime.RecordDuration(time.Since(st))
	}
}

func (s *oneDCSource) parse(buffer batch) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for _, batch := range buffer.Batches {
		res = append(res, s.oldParseBatch(batch)...)
	}
	return res
}

func (s *oneDCSource) oldParseBatch(b parsers.MessageBatch) []abstract.ChangeItem {
	firstLbOffset := b.Messages[0].Offset
	lastLbOffset := b.Messages[len(b.Messages)-1].Offset
	var ts time.Time
	totalSize := 0
	for i, m := range b.Messages {
		if firstLbOffset+uint64(i) != m.Offset {
			s.logger.Warn("Inconsistency")
		}
		totalSize += len(m.Value)
		if ts.IsZero() || m.CreateTime.Before(ts) {
			ts = m.CreateTime
		}

		if ts.IsZero() || m.WriteTime.Before(ts) {
			ts = m.WriteTime
		}
	}
	st := time.Now()
	parsed := s.parser.DoBatch(b)
	s.metrics.DecodeTime.RecordDuration(time.Since(st))
	s.metrics.ChangeItems.Add(int64(len(parsed)))
	for _, ci := range parsed {
		if ci.IsRowEvent() {
			s.metrics.Parsed.Inc()
		}
	}
	s.logger.Debug(
		fmt.Sprintf("GenericParser done in %v (%v)", time.Since(st), format.SizeInt(totalSize)),
		log.Any("#change_items", len(parsed)),
		log.Any("lb_partition", b.Partition),
		log.Any("first_lb_offset", firstLbOffset),
		log.Any("last_lb_offset", lastLbOffset),
		log.Any("session_id", s.consumer.Stat().SessionID),
	)
	return parsed
}

func NewOneDCSource(cfg *LfSource, logger log.Logger, registry *stats.SourceStats, retries int) (abstract.Source, error) {
	// In test we use logbroker with local environment therefore we should skip this check
	if !env.IsTest() {
		if instanceIsValid := checkInstanceValidity(cfg.Instance); !instanceIsValid {
			return nil, abstract.NewFatalError(xerrors.Errorf("the instance '%s' from config is not known", cfg.Instance))
		}
	}
	var topics []persqueue.TopicInfo
	if len(cfg.Topics) > 0 {
		topics = make([]persqueue.TopicInfo, len(cfg.Topics))
		for i, topic := range cfg.Topics {
			topics[i] = persqueue.TopicInfo{
				Topic:           topic,
				PartitionGroups: nil,
			}
		}
	}

	var opts persqueue.ReaderOptions
	opts.Logger = corelogadapter.New(logger)
	opts.Endpoint = string(cfg.Instance)
	opts.Database = cfg.Database
	opts.ManualPartitionAssignment = true
	opts.Consumer = cfg.Consumer
	opts.Topics = topics
	opts.ReadOnlyLocal = cfg.Cluster != "" || cfg.OnlyLocal
	opts.MaxReadSize = uint32(cfg.MaxReadSize)
	opts.MaxMemory = int(cfg.MaxMemory)
	opts.MaxTimeLag = cfg.MaxTimeLag
	opts.RetryOnFailure = true
	opts.MaxReadMessagesCount = cfg.MaxReadMessagesCount
	opts.Port = cfg.Port
	opts.Credentials = cfg.Credentials

	if cfg.TLS == EnabledTLS {
		var err error
		opts.TLSConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("unable to load tls: %w", err)
		}
	}
	currReader := persqueue.NewReader(opts)

	ctx, cancel := context.WithCancel(context.Background())
	rollbacks := util.Rollbacks{}
	rollbacks.Add(cancel)
	defer rollbacks.Do()
	counter := 0
	for {
		if init, err := currReader.Start(ctx); err != nil {
			logger.Warn("Unable to start consumer", log.Error(err))
			if counter < retries {
				counter++
				time.Sleep(time.Second)
				continue
			} else {
				return nil, xerrors.Errorf("unable to start consumer, err: %w", err)
			}
		} else {
			optsC := opts
			optsC.Credentials = nil
			logger.Info("Init logbroker session: "+init.SessionID, log.Any("opts", optsC), log.Any("init", init), log.Any("session_id", init.SessionID))
		}

		break
	}

	parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to make parser, err: %w", err)
	}
	if resourceable, ok := parser.(resources.Resourceable); ok {
		resourceable.ResourcesObj().RunWatcher()
	}

	stopCh := make(chan bool)

	p := oneDCSource{
		config:           cfg,
		parser:           parser,
		offsetsValidator: queues.NewLbOffsetsSourceValidator(logger),
		consumer:         currReader,
		cancel:           cancel,
		onceStop:         sync.Once{},
		stopCh:           stopCh,
		onceErr:          sync.Once{},
		errCh:            make(chan error, 1),
		logger:           logger,
		metrics:          registry,
		runningWG:        sync.WaitGroup{},
		lastRead:         time.Now(),
		maxBatchSize:     15 * 1024 * 1024,
	}

	if cfg.MaxIdleTime > 0 {
		go p.monitorIdle(cfg.MaxIdleTime)
	}

	rollbacks.Cancel()
	return &p, nil
}

func checkInstanceValidity(configInstance LogbrokerInstance) bool {
	for _, knownInstances := range KnownClusters {
		if slices.Contains(knownInstances, configInstance) {
			return true
		}
	}
	return false
}
