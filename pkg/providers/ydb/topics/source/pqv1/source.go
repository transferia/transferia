package pqv1source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/functions"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/resources"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/queues"
	"github.com/transferia/transferia/pkg/util/queues/lbyds"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

type Source struct {
	config           *topicsource.Config
	offsetsValidator *lbyds.LbOffsetsSourceValidator
	consumer         persqueue.Reader
	cancel           context.CancelFunc

	parser parsers.Parser

	onceStop sync.Once
	stopCh   chan bool

	metrics *stats.SourceStats
	logger  log.Logger

	executor            *functions.Executor
	commitLatencyLogger *commitLatencyLogger
}

func (s *Source) YSRNamespaceID() string {
	if srParser, ok := s.parser.(*parsers.YSRableParser); ok {
		return srParser.YSRNamespaceID()
	}
	return ""
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	var transformFunc lbyds.TransformFunc
	if s.executor != nil {
		transformFunc = func(data []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
			st := time.Now()
			s.logger.Infof("begin transform for batches %v rows", len(data))
			transformed, err := s.executor.Do(data)
			if err != nil {
				return nil, xerrors.Errorf("Cloud function transformation error in %v, %v rows -> %v rows, err: %w", time.Since(st), len(data), len(transformed), err)
			}
			s.logger.Infof("Cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
			s.metrics.TransformTime.RecordDuration(time.Since(st))

			return transformed, nil
		}
	}

	parseWrapper := func(batch committableBatch) ([]abstract.ChangeItem, error) {
		if len(batch.Batches) == 0 {
			return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}, nil
		}

		return lbyds.Parse(batch.Batches, s.parser, s.metrics, s.logger, transformFunc, s.config.UseFullTopicNameForParsing)
	}
	parseQ := parsequeue.NewWaitable(s.logger, s.config.ParseQueueParallelism, sink, parseWrapper, s.ack)

	runErr := s.run(parseQ)

	parseQ.Close()
	return multierr.Combine(runErr, parseQ.Error())
}

func (s *Source) run(parseQ *parsequeue.WaitableParseQueue[committableBatch]) error {
	defer func() {
		s.consumer.Shutdown()
		lbyds.WaitSkippedMsgs(s.logger, s.consumer, "yds")
	}()

	lastPush := time.Now()
	for {
		select {
		case <-s.stopCh:
			s.logger.Warn("reader is closed")
			return nil

		case <-parseQ.Done():
			s.logger.Warn("parse queue is closed")
			return nil

		case b, ok := <-s.consumer.C():
			if !ok {
				s.logger.Warn("Reader closed")
				return xerrors.New("consumer closed, close subscription")
			}

			stat := s.consumer.Stat()
			s.metrics.Usage.Set(float64(stat.MemUsage))
			s.metrics.Read.Set(float64(stat.BytesRead))
			s.metrics.Extract.Set(float64(stat.BytesExtracted))

			switch v := b.(type) {
			case *persqueue.CommitAck:
				s.commitLatencyLogger.handleAck(v)
			case *persqueue.LockV1:
				s.lockPartition(v)
			case *persqueue.ReleaseV1:
				s.logger.Infof("Received 'Release' event, partition:%s@%d", v.Topic, v.Partition)
				err := s.sendSynchronizeEventIfNeeded(parseQ)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
				v.Release()
			case *persqueue.Disconnect:
				s.commitLatencyLogger.reset()
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
				batches := lbyds.ConvertBatches(v.Batches())
				err := s.offsetsValidator.CheckLbOffsets(batches)
				if err != nil {
					if s.config.AllowTTLRewind {
						s.logger.Warn("ttl rewind", log.Error(err))
					} else {
						s.metrics.Fatal.Inc()
						return abstract.NewFatalError(err)
					}
				}
				ranges := lbyds.BuildMapPartitionToLbOffsetsRange(batches)
				s.logger.Debug("got lb_offsets", log.Any("range", ranges))

				s.metrics.Master.Set(1)
				messagesSize, messagesCount := queues.BatchStatistics(batches)
				s.metrics.Size.Add(messagesSize)
				s.metrics.Count.Add(messagesCount)

				s.logger.Debugf("begin to process batch: %v items with %v, time from last batch: %v", len(batches), format.SizeUInt64(uint64(messagesSize)), time.Since(lastPush))
				if err := parseQ.Add(newBatch(s.commitLatencyLogger.wrap(v), batches)); err != nil {
					return xerrors.Errorf("unable to add message to parser process: %w", err)
				}
				lastPush = time.Now()
			}
		}
	}
}

func (s *Source) Stop() {
	s.onceStop.Do(func() {
		close(s.stopCh)
		s.cancel()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			s.logger.Warn("timeout in lb reader abort")
			return
		case <-s.consumer.Closed():
			s.logger.Info("abort lb reader")
			return
		}
	}
}

func (s *Source) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		b, ok := <-s.consumer.C()
		if !ok {
			return nil, xerrors.New("consumer closed, close subscription")
		}
		select {
		case <-ctx.Done():
			return nil, xerrors.New("context deadline")
		default:
		}
		switch v := b.(type) {
		case *persqueue.CommitAck:
			s.commitLatencyLogger.handleAck(v)
		case *persqueue.LockV1:
			s.lockPartition(v)
		case *persqueue.ReleaseV1:
			_ = s.sendSynchronizeEventIfNeeded(nil)
		case *persqueue.Data:
			var dataBatches [][]abstract.ChangeItem
			batchSize := 0
			var res []abstract.ChangeItem
			var data []abstract.ChangeItem
			for _, b := range lbyds.ConvertBatches(v.Batches()[:1]) {
				total := len(b.Messages)
				if len(b.Messages) > 3 {
					total = 3
				}
				for _, m := range b.Messages[:total] {
					data = append(data, lbyds.MessageAsChangeItem(m, b, false))
					batchSize += len(m.Value)
				}
				res = append(res, data...)
				dataBatches = append(dataBatches, data)
			}
			if s.executor != nil {
				res = nil
				for i := range dataBatches {
					transformed, err := s.executor.Do(dataBatches[i])
					if err != nil {
						return nil, err
					}
					dataBatches[i] = transformed
					res = append(res, transformed...)
				}
			}
			if s.parser != nil {
				res = nil
				// DO CONVERT
				for i := range dataBatches {
					var rows []abstract.ChangeItem
					for _, row := range dataBatches[i] {
						ci, part := lbyds.ChangeItemAsMessage(row)
						rows = append(rows, s.parser.Do(ci, part)...)
					}
					res = append(res, rows...)
				}
			}
			return res, nil
		case *persqueue.Disconnect:
			s.commitLatencyLogger.reset()
			if v.Err != nil {
				s.logger.Errorf("Disconnected: %s", v.Err.Error())
			} else {
				s.logger.Error("Disconnected")
			}
			continue
		default:
			continue
		}
	}
}

func (s *Source) watchParserResource(parser parsers.Parser) {
	var resource resources.AbstractResources
	if resourceable, ok := parser.(resources.Resourceable); ok {
		resource = resourceable.ResourcesObj()
	} else {
		return
	}

	select {
	case <-resource.OutdatedCh():
		s.logger.Warn("Parser resource is outdated, stop source")
		s.Stop()
	case <-s.stopCh:
		return
	}
}

func (s *Source) lockPartition(lock *persqueue.LockV1) {
	partName := fmt.Sprintf("%v@%v", lock.Topic, lock.Partition)
	s.logger.Infof("Lock partition:%v ReadOffset:%v, EndOffset:%v", partName, lock.ReadOffset, lock.EndOffset)
	s.offsetsValidator.InitOffsetForPartition(lock.Topic, uint32(lock.Partition), lock.ReadOffset)
	lock.StartRead(true, lock.ReadOffset, lock.ReadOffset)
}

func (s *Source) sendSynchronizeEventIfNeeded(parseQ *parsequeue.WaitableParseQueue[committableBatch]) error {
	if s.config.IsYDBTopicSink && parseQ != nil {
		s.logger.Info("Sending synchronize event")
		if err := parseQ.Add(newEmtpyBatch()); err != nil {
			return xerrors.Errorf("unable to add message to parser process: %w", err)
		}
		parseQ.Wait()
		s.logger.Info("Sent synchronize event")
	}
	return nil
}

func (s *Source) ack(data committableBatch, st time.Time) error {
	data.Commit()
	s.metrics.PushTime.RecordDuration(time.Since(st))

	return nil
}

func NewSource(cfg *topicsource.Config, parser parsers.Parser, logger log.Logger, metrics *stats.SourceStats) (*Source, error) {
	topics := make([]persqueue.TopicInfo, len(cfg.Topics))
	for i, topic := range cfg.Topics {
		topics[i] = persqueue.TopicInfo{
			Topic:           topic,
			PartitionGroups: nil,
		}
	}

	readerOpts := persqueue.ReaderOptions{
		Logger:                    corelogadapter.New(logger),
		Endpoint:                  cfg.Connection.Endpoint,
		Database:                  cfg.Connection.Database,
		Credentials:               cfg.Connection.Credentials,
		Topics:                    topics,
		Consumer:                  cfg.Consumer,
		ManualPartitionAssignment: true,
		RetryOnFailure:            true,
		ReadOnlyLocal:             cfg.ReaderOpts.ReadOnlyLocal,
		MaxMemory:                 cfg.ReaderOpts.MaxMemory,
		MaxReadSize:               cfg.ReaderOpts.MaxReadSize,
		MaxReadMessagesCount:      cfg.ReaderOpts.MaxReadMessageCount,
		MaxTimeLag:                cfg.ReaderOpts.MaxTimeLag,
		MinReadInterval:           cfg.ReaderOpts.MinReadInterval,
	}

	if cfg.Connection.TLSEnabled {
		tls, err := xtls.FromPath(cfg.Connection.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to obtain TLS configuration: %w", err)
		}
		readerOpts.TLSConfig = tls
	}

	c := persqueue.NewReaderV1(readerOpts)
	ctx, cancel := context.WithCancel(context.Background())
	var rb util.Rollbacks
	rb.Add(cancel)
	defer rb.Do()

	if _, err := c.Start(ctx); err != nil {
		logger.Error("failed to start reader", log.Error(err))
		return nil, xerrors.Errorf("failed to start reader: %w", err)
	}

	var executor *functions.Executor
	if cfg.Transformer != nil {
		var err error
		executor, err = functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger)
		if err != nil {
			logger.Error("failed to create a function executor", log.Error(err))
			return nil, xerrors.Errorf("failed to create a function executor: %w", err)
		}
	}

	rb.Cancel()
	stopCh := make(chan bool)

	src := &Source{
		config:              cfg,
		offsetsValidator:    lbyds.NewLbOffsetsSourceValidator(logger),
		consumer:            c,
		cancel:              cancel,
		parser:              parser,
		onceStop:            sync.Once{},
		stopCh:              stopCh,
		metrics:             metrics,
		logger:              logger,
		executor:            executor,
		commitLatencyLogger: newCommitLatencyLogger(logger),
	}

	go src.watchParserResource(parser)

	return src, nil
}
