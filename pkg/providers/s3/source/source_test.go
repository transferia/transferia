package source

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	"github.com/transferia/transferia/pkg/stats"
)

type mockSink struct {
	abstract.AsyncSink
	push func(items []abstract.ChangeItem) chan error
}

func (m *mockSink) AsyncPush(items []abstract.ChangeItem) chan error {
	return m.push(items)
}

func (m *mockSink) Close() error {
	return nil
}

type mockObjectFetcher struct {
	object_fetcher.ObjectFetcher

	cntFetchObjects int
}

func (m *mockObjectFetcher) FetchObjects(reader reader.Reader) ([]file.File, error) {
	m.cntFetchObjects++
	return []file.File{}, nil
}

func (m *mockObjectFetcher) Commit(fileName string) error {
	return nil
}

func (m *mockObjectFetcher) RunBackgroundThreads(_ chan error) {}

type mockReader struct {
	reader.Reader
}

func (m *mockReader) ParsePassthrough(chunk pusher.Chunk) []abstract.ChangeItem {
	return []abstract.ChangeItem{}
}

func TestS3Source_run_fetch_delay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	objectFetcher := &mockObjectFetcher{}
	source := &S3Source{
		objectFetcher: objectFetcher,
		fetchInterval: 450 * time.Millisecond,
		logger:        logger.Log,
		ctx:           ctx,
		errCh:         make(chan error, 1),
		metrics:       stats.NewSourceStats(metrics.NewRegistry()),
		reader:        &mockReader{},
		cancel:        func() {},
	}

	pushCnt := 0

	go func() {
		sink := &mockSink{push: func(items []abstract.ChangeItem) chan error {
			pushCnt++
			ch := make(chan error)
			go func() {
				ch <- nil
			}()
			return ch
		}}
		require.NoError(t, source.Run(sink))
	}()
	defer func() {
		cancel()
	}()

	time.Sleep(1100 * time.Millisecond)

	require.Equal(t, 2, objectFetcher.cntFetchObjects)
}

func TestS3Source_run_default_delay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	objectFetcher := &mockObjectFetcher{}
	source := &S3Source{
		objectFetcher: objectFetcher,
		fetchInterval: 0,
		logger:        logger.Log,
		ctx:           ctx,
		errCh:         make(chan error, 1),
		metrics:       stats.NewSourceStats(metrics.NewRegistry()),
		reader:        &mockReader{},
		cancel:        func() {},
	}

	pushCnt := 0

	go func() {
		sink := &mockSink{push: func(items []abstract.ChangeItem) chan error {
			pushCnt++
			ch := make(chan error)
			go func() {
				ch <- nil
			}()
			return ch
		}}
		require.NoError(t, source.Run(sink))
	}()
	defer func() {
		cancel()
	}()

	time.Sleep(5000 * time.Millisecond)

	require.GreaterOrEqual(t, 5, objectFetcher.cntFetchObjects)
}

func TestS3Source_newBackoffForFetchInterval(t *testing.T) {
	source := &S3Source{
		fetchInterval: 450 * time.Millisecond,
		logger:        logger.Log,
	}

	backoffForFetchInterval := source.newBackoffForFetchInterval()
	require.IsType(t, &backoff.ConstantBackOff{}, backoffForFetchInterval)
	require.Equal(t, 450*time.Millisecond, backoffForFetchInterval.NextBackOff())

	source.fetchInterval = 0
	backoffForFetchInterval = source.newBackoffForFetchInterval()
	require.IsType(t, &backoff.ExponentialBackOff{}, backoffForFetchInterval)
}
