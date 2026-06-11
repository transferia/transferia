package topicapisource

import (
	"context"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/functions"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/resources"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader/event"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/queues"
	"github.com/transferia/transferia/pkg/util/queues/lbyds"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.ytsaurus.tech/library/go/core/log"
)

type Source struct {
	config *topicsource.Config

	ydbClient        *ydb.Driver
	reader           eventreader.EventReader
	offsetsValidator *lbyds.LbOffsetsSourceValidator

	parser        parsers.Parser
	cloudFunction *functions.Executor

	onceStop sync.Once
	stopCh   chan struct{}

	logger  log.Logger
	metrics *stats.SourceStats
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.NewWaitable[*event.ReadEvent](s.logger, s.config.ParseQueueParallelism, sink, s.parserWithCloudFunc(), s.ack)

	runErr := s.run(parseQ)

	parseQ.Close()
	return multierr.Combine(runErr, parseQ.Error())
}

func (s *Source) run(parseQ parsequeue.WaitableQueue[*event.ReadEvent]) error {
	lastPush := time.Now()
	for {
		select {
		case <-s.stopCh:
			return nil
		case <-parseQ.Done():
			return nil
		default:
		}

		nextEvent, err := func() (event.Event, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			return s.reader.NextEvent(ctx)
		}()
		if err != nil && !xerrors.Is(err, context.DeadlineExceeded) {
			return xerrors.Errorf("unable to read next event: %w", err)
		}

		switch typedEvent := nextEvent.(type) {
		case *event.StartEvent:
			s.logger.Info("start reading partition",
				log.String("topic", typedEvent.PartitionInfo.TopicPath),
				log.Int64("partition", typedEvent.PartitionInfo.PartitionID),
			)
			typedEvent.Confirm()

		case *event.StopEvent:
			s.logger.Info("stop reading partition",
				log.String("topic", typedEvent.PartitionInfo.TopicPath),
				log.Int64("partition", typedEvent.PartitionInfo.PartitionID),
			)
			if err := s.sendSynchronizeEventIfNeeded(parseQ); err != nil {
				return xerrors.Errorf("failed to send synchronize event: %w", err)
			}
			typedEvent.Confirm()

		case *event.ReadEvent:
			batches := []parsers.MessageBatch{typedEvent.Batch}
			if err := s.offsetsValidator.CheckLbOffsets(batches); err != nil {
				if s.config.AllowTTLRewind {
					s.logger.Warn("message TTL rewind detected", log.Error(err))
				} else {
					s.metrics.Fatal.Inc()
					return abstract.NewFatalError(err)
				}
			}
			offsetRanges := lbyds.BuildMapPartitionToLbOffsetsRange(batches)
			s.logger.Debug("got topic offsets", log.Any("range", offsetRanges))

			s.metrics.Master.Set(1)
			messagesSize, messagesCount := queues.BatchStatistics(batches)
			s.metrics.Size.Add(messagesSize)
			s.metrics.Count.Add(messagesCount)

			s.logger.Debug("processing batch",
				log.String("size", format.SizeUInt64(uint64(messagesSize))),
				log.Duration("time_since_last", time.Since(lastPush)))
			if err := parseQ.Add(typedEvent); err != nil {
				return xerrors.Errorf("failed to add read event to queue: %w", err)
			}
			lastPush = time.Now()
		}
	}
}

func (s *Source) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		nextEvent, err := s.reader.NextEvent(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to fetch event: %w", err)
		}

		switch typedEvent := nextEvent.(type) {
		case *event.StartEvent:
			typedEvent.Confirm()
		case *event.StopEvent:
			typedEvent.Confirm()
		case *event.ReadEvent:
			parseF := s.parserWithCloudFunc()

			result, err := parseF(typedEvent)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse fetched event: %w", err)
			}

			return result, nil
		}
	}
}

func (s *Source) Stop() {
	s.onceStop.Do(func() {
		close(s.stopCh)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.reader.Close(ctx); err != nil {
		s.logger.Warn("failed to close reader", log.Error(err))
	}
	if err := s.ydbClient.Close(ctx); err != nil {
		s.logger.Warn("failed to close ydb client", log.Error(err))
	}
}

func (s *Source) sendSynchronizeEventIfNeeded(parseQ parsequeue.WaitableQueue[*event.ReadEvent]) error {
	if !s.config.IsYDBTopicSink {
		return nil
	}

	s.logger.Info("sending synchronize event")
	if err := parseQ.Add(nil); err != nil {
		return err
	}
	parseQ.Wait()

	return nil
}

func (s *Source) parserWithCloudFunc() func(*event.ReadEvent) ([]abstract.ChangeItem, error) {
	var transformFunc lbyds.TransformFunc
	if s.cloudFunction != nil {
		transformFunc = func(data []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
			st := time.Now()
			transformed, err := s.cloudFunction.Do(data)
			if err != nil {
				return nil, xerrors.Errorf("cloud function transformation error in %v, %v rows -> %v rows, err: %w", time.Since(st), len(data), len(transformed), err)
			}
			s.logger.Infof("cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
			s.metrics.TransformTime.RecordDuration(time.Since(st))

			return transformed, nil
		}
	}

	return func(readEvent *event.ReadEvent) ([]abstract.ChangeItem, error) {
		if readEvent == nil {
			return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}, nil
		}

		return lbyds.Parse([]parsers.MessageBatch{readEvent.Batch}, s.parser, s.metrics, s.logger, transformFunc, s.config.UseFullTopicNameForParsing)
	}
}

func (s *Source) ack(readEvent *event.ReadEvent, st time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := readEvent.Commit(ctx); err != nil {
		return xerrors.Errorf("failed to commit event: %w", err)
	}
	s.metrics.PushTime.RecordDuration(time.Since(st))

	return nil
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
		s.logger.Warn("parser resource is outdated, stop source")
		s.Stop()
	case <-s.stopCh:
		return
	}
}

func newBaseSource(cfg *topicsource.Config, parser parsers.Parser, ydbClient *ydb.Driver, reader eventreader.EventReader, logger log.Logger, metrics *stats.SourceStats) (*Source, error) {
	var executor *functions.Executor
	if cfg.Transformer != nil {
		var err error
		executor, err = functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger)
		if err != nil {
			logger.Error("failed to create a function executor", log.Error(err))
			return nil, xerrors.Errorf("failed to create a function executor: %w", err)
		}
	}

	src := &Source{
		config:           cfg,
		ydbClient:        ydbClient,
		reader:           reader,
		offsetsValidator: lbyds.NewLbOffsetsSourceValidator(logger),
		parser:           parser,
		cloudFunction:    executor,
		onceStop:         sync.Once{},
		stopCh:           make(chan struct{}),
		logger:           logger,
		metrics:          metrics,
	}

	go src.watchParserResource(parser)

	return src, nil
}

func NewSource(cfg *topicsource.Config, parser parsers.Parser, logger log.Logger, metrics *stats.SourceStats) (*Source, error) {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	ydbClient, err := topiccommon.NewYDBDriver(cfg.Connection, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb client: %w", err)
	}
	rollbacks.Add(func() {
		_ = ydbClient.Close(context.Background())
	})

	reader, err := eventreader.NewTopicEventReader(cfg.Consumer, cfg.Topics, ydbClient, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create event reader: %w", err)
	}
	rollbacks.Add(func() {
		_ = reader.Close(context.Background())
	})

	src, err := newBaseSource(cfg, parser, ydbClient, reader, logger, metrics)
	if err != nil {
		return nil, err
	}

	rollbacks.Cancel()

	return src, nil
}
