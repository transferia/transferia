package sample

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	userActivitiesSampleType = "user_activities"
)

var (
	_ abstract.Source = (*Source)(nil)
)

type Source struct {
	src        *SampleSource
	ctx        context.Context
	cancel     func()
	metrics    *stats.SourceStats
	transferID string
	logger     log.Logger
	wg         sync.WaitGroup
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	sleepTimer := backoff.NewExponentialBackOff()
	sleepTimer.InitialInterval = s.src.MinSleepTime
	sleepTimer.MaxElapsedTime = 0
	sleepTimer.Reset()

	defer s.cancel()
	errCh := make(chan error)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			s.wg.Wait()
			close(errCh)
			return nil
		case err := <-errCh:
			if err != nil {
				return xerrors.Errorf("received async push error from generated data: %w", err)
			}
		default:
		}

		nextSleepDuration := sleepTimer.NextBackOff()
		if nextSleepDuration > 2*s.src.MinSleepTime {
			sleepTimer.Reset()
			nextSleepDuration = s.src.MinSleepTime
		}
		s.logger.Infof("sleep duration %v", nextSleepDuration)
		time.Sleep(nextSleepDuration)

		data := generateRandomDataForSampleType(s.src.SampleType, s.src.MaxSampleData, s.src.TableName, s.metrics)
		s.metrics.ChangeItems.Add(int64(len(data)))

		pushStart := time.Now()
		s.wg.Add(1)
		go func(errChAsync chan error, pushStart time.Time) {
			defer s.wg.Done()
			err := <-errChAsync
			if err != nil {
				ok := util.Send(s.ctx, errCh, err)
				if !ok {
					s.logger.Error("context was canceled")
					return
				}
				s.logger.Info("successfully pushed error into ErrCh channel")
			} else {
				s.metrics.PushTime.RecordDuration(time.Since(pushStart))
			}
		}(sink.AsyncPush(data), pushStart)
	}
}

func generateRandomDataForSampleType(sampleType string, maxRowSampleData int64, table string, metrics *stats.SourceStats) []abstract.ChangeItem {
	generatedData := make([]abstract.ChangeItem, maxRowSampleData)
	var streamData StreamingData

	for i := 0; i < int(maxRowSampleData); i++ {
		switch sampleType {
		case userActivitiesSampleType:
			streamData = NewUserActivities(table)
		default:
			streamData = NewIot(table)
		}

		generatedData[i] = streamData.ToChangeItem(int64(i))
		metrics.Size.Add(int64(generatedData[i].Size.Read))
		metrics.Count.Inc()
	}

	return generatedData
}

func (s *Source) Stop() {
	s.cancel()
}

func NewSource(src *SampleSource, transferID string, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	metric := stats.NewSourceStats(registry)
	ctx, cancel := context.WithCancel(context.Background())

	return &Source{
		src:        src,
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metric,
		transferID: transferID,
		logger:     logger,
		wg:         sync.WaitGroup{},
	}, nil
}
