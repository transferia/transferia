package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"go.ytsaurus.tech/library/go/core/log"
)

// Record wraps the record returned from the Kinesis library and
// extends to include the shard id.
type Record struct {
	*kinesis.Record
	ShardID            string
	MillisBehindLatest *int64
}

func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, xerrors.New("must provide stream name")
	}

	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: kinesis.ShardIteratorTypeLatest,
		initialTimestamp:         nil,
		client:                   nil,
		group:                    nil,
		logger:                   logger.Log,
		store:                    &noopStore{},
		scanInterval:             5 * time.Millisecond,
		maxRecords:               10000,
		shardClosedHandler:       nil,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.client == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return nil, err
		}
		c.client = kinesis.New(newSession)
	}

	if c.group == nil {
		c.group = NewAllGroup(c.client, c.store, streamName, c.logger)
	}

	return c, nil
}

type Consumer struct {
	streamName               string
	initialShardIteratorType string
	initialTimestamp         *time.Time
	client                   kinesisiface.KinesisAPI
	group                    Group
	logger                   log.Logger
	store                    Store
	scanInterval             time.Duration
	maxRecords               int64
	shardClosedHandler       ShardClosedHandler
}

type ScanFunc func([]*Record) error

func (c *Consumer) SetCheckpoint(shardID, sequenceNumber string) error {
	return c.store.SetCheckpoint(c.streamName, shardID, sequenceNumber)
}

// Scan launches a goroutine to process each of the shards in the stream. The ScanFunc
// is passed through to each of the goroutines and called with each message pulled from
// the stream.
func (c *Consumer) Scan(ctx context.Context, fn ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errc   = make(chan error, 1)
		shardc = make(chan *kinesis.Shard, 1)
	)

	go func() {
		c.group.Start(ctx, shardc)
		<-ctx.Done()
		close(shardc)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	for shard := range shardc {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			if err := c.ScanShard(ctx, shardID, fn); err != nil {
				select {
				case errc <- xerrors.Errorf("shard %s error: %w", shardID, err):
					// first error to occur
					cancel()
				default:
					// error has already occurred
				}
			}
		}(aws.StringValue(shard.ShardId))
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	if err != nil {
		return xerrors.Errorf("get checkpoint error: %w", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return xerrors.Errorf("get shard iterator error: %w", err)
	}

	c.logger.Infof("start scan: %s / %v", shardID, lastSeqNum)
	defer func() {
		c.logger.Infof("stop scan: %s", shardID)
	}()
	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
			Limit:         aws.Int64(c.maxRecords),
			ShardIterator: shardIterator,
		})
		// attempt to recover from GetRecords error when expired iterator
		if err != nil {
			c.logger.Warn("get records error", log.Error(err))

			if awserr, ok := err.(awserr.Error); ok {
				if _, ok := retriableErrors[awserr.Code()]; !ok {
					return xerrors.Errorf("get records error: %v", awserr.Message())
				}
			}

			shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return xerrors.Errorf("get shard iterator error: %w", err)
			}
		} else {
			err = fn(yslices.Map(resp.Records, func(r *kinesis.Record) *Record {
				lastSeqNum = *r.SequenceNumber
				return &Record{r, shardID, resp.MillisBehindLatest}
			}))
			if err != nil {
				return xerrors.Errorf("unable to process records: %w", err)
			}
		}

		if isShardClosed(resp.NextShardIterator, shardIterator) {
			c.logger.Infof("shard closed %s", shardID)
			if c.shardClosedHandler != nil {
				err := c.shardClosedHandler(c.streamName, shardID)
				if err != nil {
					return xerrors.Errorf("shard closed handler error: %w", err)
				}
			}
			return nil
		}

		shardIterator = resp.NextShardIterator

		// Wait for next scan
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			continue
		}
	}
}

var retriableErrors = map[string]struct{}{
	kinesis.ErrCodeExpiredIteratorException:               {},
	kinesis.ErrCodeProvisionedThroughputExceededException: {},
	kinesis.ErrCodeInternalFailureException:               {},
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, seqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		params.StartingSequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAtTimestamp)
		params.Timestamp = c.initialTimestamp
	} else {
		params.ShardIteratorType = aws.String(c.initialShardIteratorType)
	}

	res, err := c.client.GetShardIteratorWithContext(ctx, params)
	return res.ShardIterator, err
}
