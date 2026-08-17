package source

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	"github.com/transferia/transferia/pkg/stats"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

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

type oneObjectFetcher struct {
	object_fetcher.ObjectFetcher
	fetched bool
}

func (m *oneObjectFetcher) FetchObjects(reader.Reader) ([]file.File, error) {
	if m.fetched {
		return nil, nil
	}
	m.fetched = true
	return []file.File{{FileName: "file.log"}}, nil
}

func (m *oneObjectFetcher) Commit(string) error {
	return nil
}

func (m *oneObjectFetcher) RunBackgroundThreads(_ chan error) {}

type oneChunkReader struct {
	reader.Reader
}

func (*oneChunkReader) Read(ctx context.Context, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	err := pusher.Push(ctx, s3_pusher.NewChunk(filePath, true, 0, 1, []abstract.ChangeItem{{}}))
	if err != nil {
		return reader_error.NewReaderErrorSink("oneChunkReader.Read", filePath, err)
	}
	return nil
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
		sink := mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			pushCnt++
			return nil
		})
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
		sink := mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			pushCnt++
			return nil
		})
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

func TestS3SourceRunReturnsOnSinkFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &S3Source{
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger.Log,
		srcModel:      &s3_model.S3Source{Concurrency: 1},
		metrics:       stats.NewSourceStats(metrics.NewRegistry()),
		reader:        &oneChunkReader{},
		objectFetcher: &oneObjectFetcher{},
		errCh:         make(chan error, 1),
		fetchInterval: time.Millisecond,
	}

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- source.Run(mocksink.NewMockAsyncSink(func([]abstract.ChangeItem) error {
			return xerrors.New("sink failed")
		}))
	}()

	select {
	case err := <-runErrCh:
		require.ErrorContains(t, err, "sink failed")
	case <-time.After(5 * time.Second):
		t.Fatal("S3 source did not stop after its parse queue failed")
	}
}
