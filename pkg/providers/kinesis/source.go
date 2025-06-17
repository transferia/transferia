//go:build !disable_kinesis_provider

package kinesis

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/kinesis/consumer"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ abstract.Source    = (*Source)(nil)
	_ abstract.Fetchable = (*Source)(nil)
)

type Source struct {
	registry metrics.Registry
	cp       coordinator.Coordinator
	logger   log.Logger
	config   *KinesisSource
	metrics  *stats.SourceStats
	parser   parsers.Parser

	inflightMutex sync.Mutex
	inflightBytes int

	ctx       context.Context
	cancel    func()
	consumer  *consumer.Consumer
	lastError error
}

func (s *Source) Fetch() ([]abstract.ChangeItem, error) {
	stapErr := xerrors.New("stap")
	var res []abstract.ChangeItem
	err := s.consumer.Scan(s.ctx, func(data []*consumer.Record) error {
		parsed := s.parse(data)
		if len(parsed) == 0 {
			return nil
		}
		res = append(res, parsed...)
		if len(res) >= 3 {
			return stapErr
		}
		return nil
	})
	if xerrors.Is(err, stapErr) {
		err = nil
	}
	return res, err
}

func (s *Source) inLimits() bool {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()
	return s.config.BufferSize == 0 || int(s.config.BufferSize) > s.inflightBytes
}

func (s *Source) addInflight(size int) {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()
	s.inflightBytes += size
}

func (s *Source) reduceInflight(size int) {
	s.inflightMutex.Lock()
	defer s.inflightMutex.Unlock()
	s.inflightBytes = s.inflightBytes - size
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.NewWaitable(s.logger, s.config.ParseQueueParallelism, sink, s.parse, s.ack)
	defer parseQ.Close()

	return s.run(parseQ)
}

func (s *Source) run(parseQ *parsequeue.WaitableParseQueue[[]*consumer.Record]) error {
	for {
		s.metrics.Master.Set(1)
		s.waitLimits()
		err := s.consumer.Scan(s.ctx, func(data []*consumer.Record) error {
			if len(data) == 0 {
				return nil
			}
			for _, row := range data {
				s.addInflight(len(row.Data))
			}
			return parseQ.Add(data)
		})
		if err != nil {
			return xerrors.Errorf("unable to scan: %w", err)
		}
		if s.lastError != nil {
			return xerrors.Errorf("unable to push: %w", s.lastError)
		}
	}
}

func (s *Source) ack(data []*consumer.Record, pushSt time.Time, err error) {
	if err != nil {
		s.lastError = err
		s.cancel()
	}
	offsets := map[string]string{}
	for _, r := range data {
		offsets[r.ShardID] = *r.SequenceNumber
	}
	for shardID, seqNo := range offsets {
		_ = s.consumer.SetCheckpoint(shardID, seqNo)
	}
	for _, row := range data {
		s.reduceInflight(len(row.Data))
	}
	s.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (s *Source) parse(record []*consumer.Record) []abstract.ChangeItem {
	var data []abstract.ChangeItem
	totalSize := 0
	for _, msg := range record {
		totalSize += len(msg.Data)
		s.metrics.Size.Add(int64(len(msg.Data)))
		s.metrics.Count.Inc()
		data = append(data, s.makeRawChangeItem(msg))
		msg.Data = nil
	}
	s.logger.Infof("begin transform for batches %v, total size: %v", len(data), format.SizeInt(totalSize))
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
	s.metrics.ChangeItems.Add(int64(len(data)))
	for _, ci := range data {
		if ci.IsRowEvent() {
			s.metrics.Parsed.Inc()
		}
	}
	return data
}

func (s *Source) changeItemAsMessage(ci abstract.ChangeItem) (parsers.Message, abstract.Partition) {
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

func (s *Source) makeRawChangeItem(msg *consumer.Record) abstract.ChangeItem {
	return abstract.MakeRawMessage(
		s.config.Stream,
		*msg.ApproximateArrivalTimestamp,
		s.config.Stream,
		splitShard(msg.ShardID),
		hash(*msg.SequenceNumber),
		msg.Record.Data,
	)
}

func splitShard(shardID string) int {
	parts := strings.Split(shardID, "-")
	res, _ := strconv.Atoi(parts[1])
	return res
}

const (
	ExpectedBitLength = 186
	SequenceMask      = (1 << 4) - 1
)

func hash(id string) int64 {
	algorithm := fnv.New64a()
	_, _ = algorithm.Write([]byte(id))
	return int64(algorithm.Sum64())
}

func (s *Source) Stop() {
	s.cancel()
}

func (s *Source) waitLimits() {
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for !s.inLimits() {
		time.Sleep(time.Millisecond * 10)
		if s.ctx.Err() != nil {
			s.logger.Warn("context aborted, stop wait for limits")
			return
		}
		if time.Since(logTime) > nextLogDuration {
			logTime = time.Now()
			nextLogDuration = backoffTimer.NextBackOff()
			s.logger.Warnf(
				"reader throttled for %v, limits: %v / %v",
				backoffTimer.GetElapsedTime(),
				format.SizeInt(s.inflightBytes),
				format.SizeInt(int(s.config.BufferSize)),
			)
		}
	}
}

func NewSource(
	transferID string,
	cp coordinator.Coordinator,
	cfg *KinesisSource,
	logger log.Logger,
	registry metrics.Registry,
) (*Source, error) {
	cred := credentials.AnonymousCredentials
	if cfg.AccessKey != "" {
		cred = credentials.NewStaticCredentials(cfg.AccessKey, string(cfg.SecretKey), "")
	}
	awsCfg := aws.NewConfig().
		WithRegion(cfg.Region).
		WithLogLevel(3).
		WithCredentials(cred)
	if cfg.Endpoint != "" {
		awsCfg.WithEndpoint(cfg.Endpoint)
	}
	ksis := kinesis.New(session.Must(session.NewSession(awsCfg)))

	store := consumer.NewCoordinatorStore(cp, transferID)
	c, err := consumer.New(
		cfg.Stream,
		consumer.WithStore(store),
		consumer.WithClient(ksis),
		consumer.WithShardIteratorType(kinesis.ShardIteratorTypeTrimHorizon),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to start consumer: %w", err)
	}

	var parser parsers.Parser
	if cfg.ParserConfig != nil {
		parser, err = parsers.NewParserFromMap(cfg.ParserConfig, false, logger, stats.NewSourceStats(registry))
		if err != nil {
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Source{
		registry:      registry,
		cp:            cp,
		logger:        logger,
		config:        cfg,
		metrics:       stats.NewSourceStats(registry),
		parser:        parser,
		inflightMutex: sync.Mutex{},
		inflightBytes: 0,
		ctx:           ctx,
		cancel:        cancel,
		consumer:      c,
		lastError:     nil,
	}, nil
}
