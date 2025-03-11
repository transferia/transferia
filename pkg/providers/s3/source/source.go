package source

import (
	"context"
	"fmt"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/parsequeue"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/pkg/providers/s3/pusher"
	"github.com/transferria/transferria/pkg/providers/s3/reader"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Source = (*s3Source)(nil)

type s3Source struct {
	ctx           context.Context
	cancel        func()
	src           *s3.S3Source
	transferID    string
	client        s3iface.S3API
	logger        log.Logger
	metrics       *stats.SourceStats
	reader        reader.Reader
	cp            coordinator.Coordinator
	objectFetcher ObjectFetcher
	errCh         chan error
	pusher        pusher.Pusher
	inflightLimit int64
}

var (
	ReadProgressKey = "ReadProgressKey"
	CreationEvent   = "ObjectCreated:"
	TestEvent       = "s3:TestEvent"
)

func (s *s3Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.New(s.logger, 10, sink, s.reader.ParsePassthrough, s.ack)
	return s.run(parseQ)
}

func (s *s3Source) run(parseQ *parsequeue.ParseQueue[pusher.Chunk]) error {
	defer s.metrics.Master.Set(0)
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.InitialInterval = time.Second * 10
	backoffTimer.MaxElapsedTime = 0
	backoffTimer.Reset()
	nextWaitDuration := backoffTimer.NextBackOff()

	pusher := pusher.New(nil, parseQ, s.logger, s.inflightLimit)
	s.pusher = pusher

	sqsFetcher, ok := s.objectFetcher.(*sqsSource)
	if ok {
		// enable heartbeat for message visibility
		go sqsFetcher.visibilityHeartbeat(s.errCh)
	}

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		case err := <-s.errCh:
			s.cancel() // after first error cancel ctx, so any other errors would be dropped, but not deadlocked
			return xerrors.Errorf("failed during run: %w", err)
		default:
		}
		s.metrics.Master.Set(1)
		objectList, err := s.objectFetcher.FetchObjects()
		if err != nil {
			return xerrors.Errorf("failed to get list of new objects: %w", err)
		}

		if len(objectList) == 0 {
			s.logger.Infof("No new file from s3 found, will wait %v", nextWaitDuration)
			time.Sleep(nextWaitDuration)
			nextWaitDuration = backoffTimer.NextBackOff()
			continue
		}
		backoffTimer.Reset()

		if err := util.ParallelDoWithContextAbort(s.ctx, len(objectList), int(s.src.Concurrency), func(i int, ctx context.Context) error {
			singleObject := objectList[i]
			return s.reader.Read(ctx, singleObject.Name, pusher)
		}); err != nil {
			return xerrors.Errorf("failed to read and push object: %w", err)
		}

		// reading did not result in issues but pushing might still fail
	}
}

func (s *s3Source) ack(chunk pusher.Chunk, pushSt time.Time, err error) {
	if err != nil {
		util.Send(s.ctx, s.errCh, err)
		return
	}

	// ack chunk and check if reading of file is done
	done, err := s.pusher.Ack(chunk)
	if err != nil {
		util.Send(s.ctx, s.errCh, err)
		return
	}

	if done {
		// commit this file
		if err := s.objectFetcher.Commit(Object{
			Name:         chunk.FilePath,
			LastModified: time.Now(),
		}); err != nil {
			util.Send(s.ctx, s.errCh, err)
			return
		}
	}

	s.logger.Info(
		fmt.Sprintf("Commit read changes done in %v", time.Since(pushSt)),
		log.Int("committed", len(chunk.Items)),
	)
	s.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (s *s3Source) Stop() {
	s.cancel()
}

func NewSource(src *s3.S3Source, transferID string, logger log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	sess, err := s3.NewAWSSession(logger, src.Bucket, src.ConnectionConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	metrics := stats.NewSourceStats(registry)

	reader, err := reader.New(src, logger, sess, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := aws_s3.New(sess)

	fetcher, err := NewObjectFetcher(ctx, client, logger, cp, transferID, reader, sess, src)
	if err != nil {
		cancel()
		return nil, xerrors.Errorf("failed to initialize new object fetcher: %w", err)
	}

	return &s3Source{
		src:           src,
		ctx:           ctx,
		transferID:    transferID,
		cancel:        cancel,
		reader:        reader,
		logger:        logger,
		metrics:       metrics,
		client:        client,
		cp:            cp,
		objectFetcher: fetcher,
		errCh:         make(chan error, 1),
		pusher:        nil,
		inflightLimit: src.InflightLimit,
	}, nil
}
