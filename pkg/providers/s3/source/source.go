package source

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Source = (*S3Source)(nil)

type S3Source struct {
	ctx           context.Context
	cancel        func()
	logger        log.Logger
	srcModel      *s3.S3Source
	transferID    string
	metrics       *stats.SourceStats
	reader        reader.Reader
	objectFetcher object_fetcher.ObjectFetcher
	errCh         chan error
	pusher        pusher.Pusher
	inflightLimit int64
	fetchInterval time.Duration
}

func (s *S3Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.New(s.logger, 10, sink, s.reader.ParsePassthrough, s.ack)
	return s.run(parseQ)
}

func (s *S3Source) waitPusherEmpty() {
	for {
		if s.pusher.IsEmpty() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *S3Source) sendSynchronizeEvent() error {
	err := s.pusher.Push(
		s.ctx,
		pusher.Chunk{
			FilePath:  "",
			Completed: true,
			Offset:    0,
			Size:      0,
			Items:     []abstract.ChangeItem{abstract.MakeSynchronizeEvent()},
		},
	)
	if err != nil {
		return xerrors.Errorf("failed to push synchronize event: %w", err)
	}
	s.waitPusherEmpty()
	return nil
}

func (s *S3Source) newBackoffForFetchInterval() backoff.BackOff {
	if s.fetchInterval > 0 {
		s.logger.Infof("Using fixed fetch interval: %v", s.fetchInterval)
		return backoff.NewConstantBackOff(s.fetchInterval)
	}

	s.logger.Infof("Using exponential backoff timer")
	exponentialBackoff := util.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = time.Second
	exponentialBackoff.MaxInterval = time.Minute * 10 // max delay between fetch objects
	exponentialBackoff.Multiplier = 1.5               // increase delay in 1.5 times when no files found
	exponentialBackoff.Reset()
	return exponentialBackoff
}

func (s *S3Source) run(parseQ *parsequeue.ParseQueue[pusher.Chunk]) error {
	defer s.metrics.Master.Set(0)

	fetchDelayTimer := s.newBackoffForFetchInterval()
	nextFetchDelay := fetchDelayTimer.NextBackOff()

	currPusher := pusher.New(nil, parseQ, s.logger, s.inflightLimit)
	s.pusher = currPusher

	s.objectFetcher.RunBackgroundThreads(s.errCh)

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

		if nextFetchDelay > 0 {
			s.logger.Infof("Waiting %v before fetching objects to reduce source load", nextFetchDelay)
			time.Sleep(nextFetchDelay)
		}

		objectList, err := s.objectFetcher.FetchObjects(s.reader)
		if err != nil {
			return xerrors.Errorf("failed to get list of new objects: %w", err)
		}

		if len(objectList) == 0 {
			if err := s.sendSynchronizeEvent(); err != nil {
				return xerrors.Errorf("failed to send synchronize event: %w", err)
			}
			nextFetchDelay = fetchDelayTimer.NextBackOff()
			s.logger.Infof("No new s3 files found, increasing fetch delay to %v", nextFetchDelay)

			continue
		}

		fetchDelayTimer.Reset()
		nextFetchDelay = fetchDelayTimer.NextBackOff()
		s.logger.Infof("New files found (%d), next fetch delay: %v", len(objectList), nextFetchDelay)

		err = util.ParallelDoWithContextAbort(s.ctx, len(objectList), int(s.srcModel.Concurrency), func(i int, ctx context.Context) error {
			singleObject := objectList[i].FileName
			return s.reader.Read(ctx, singleObject, currPusher)
		})
		if err != nil {
			return xerrors.Errorf("failed to read and push object: %w", err)
		}

		// reading did not result in issues but pushing might still fail

		s.waitPusherEmpty()
	}
}

func (s *S3Source) ack(chunk pusher.Chunk, pushSt time.Time, err error) {
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

	if done && chunk.FilePath != "" {
		// commit this file
		err = s.objectFetcher.Commit(chunk.FilePath)
		if err != nil {
			util.Send(s.ctx, s.errCh, err)
			return
		}
	}

	s.logger.Debug(
		fmt.Sprintf("Commit read changes done in %v", time.Since(pushSt)),
		log.Int("committed", len(chunk.Items)),
	)
	s.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (s *S3Source) Stop() {
	s.cancel()
}

func NewSource(
	srcModel *s3.S3Source,
	transferID string,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
	runtimeParallelism abstract.ShardingTaskRuntime,
) (abstract.Source, error) {
	fetcher, ctx, cancel, currReader, currMetrics, err := object_fetcher.NewWrapped(
		context.Background(),
		logger,
		registry,
		srcModel,
		transferID,
		cp,
		runtimeParallelism,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create object fetcher, err: %w", err)
	}
	return &S3Source{
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
		srcModel:      srcModel,
		transferID:    transferID,
		metrics:       currMetrics,
		reader:        currReader,
		objectFetcher: fetcher,
		errCh:         make(chan error, 1),
		pusher:        nil,
		inflightLimit: srcModel.InflightLimit,
		fetchInterval: srcModel.FetchInterval,
	}, nil
}
