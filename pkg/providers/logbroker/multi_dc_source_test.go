package logbroker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
)

func mockTemporaryError(code int) *persqueue.Error {
	return &persqueue.Error{
		Code:        code,
		Description: "mock",
	}
}

type controlledSource struct {
	run     func() error
	fetch   func() ([]abstract.ChangeItem, error)
	stopped chan struct{}
	once    sync.Once
}

var _ abstract.Source = (*controlledSource)(nil)
var _ abstract.Fetchable = (*controlledSource)(nil)

func (s *controlledSource) Run(abstract.AsyncSink) error          { return s.run() }
func (s *controlledSource) Fetch() ([]abstract.ChangeItem, error) { return s.fetch() }
func (s *controlledSource) Stop()                                 { s.once.Do(func() { close(s.stopped) }) }

func newControlledMultiSource(t *testing.T) *multiDcSource {
	source, err := NewMultiDCSource(&LfSource{Cluster: Logbroker}, &nop.Logger{}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	multi := source.(*multiDcSource)
	t.Cleanup(multi.Stop)
	return multi
}

func TestMultiSourceUsesDiscoveredInstancesAndCancelsPendingStart(t *testing.T) {
	multi := newControlledMultiSource(t)
	multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
		return []LogbrokerInstance{"new-first.example:2135", "new-second.example:2135"}, nil
	}
	pendingStarted := make(chan struct{})
	first := &controlledSource{stopped: make(chan struct{})}
	first.run = func() error {
		<-pendingStarted
		return xerrors.New("reader failed")
	}
	multi.newSource = func(ctx context.Context, cfg *LfSource, _ log.Logger, _ core_metrics.Registry) (abstract.Source, error) {
		if cfg.Instance == "new-first.example:2135" {
			return first, nil
		}
		close(pendingStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- multi.Run(nil) }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "reader failed")
	case <-time.After(5 * time.Second):
		t.Fatal("source did not cancel the pending reader start")
	}
	select {
	case <-first.stopped:
	default:
		t.Fatal("started reader was not stopped")
	}
	multi.Stop()
	multi.Stop()
}

func TestMultiSourceDiscoveryErrorDoesNotStartReaders(t *testing.T) {
	multi := newControlledMultiSource(t)
	multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
		return nil, xerrors.New("discovery unavailable")
	}
	multi.newSource = func(context.Context, *LfSource, log.Logger, core_metrics.Registry) (abstract.Source, error) {
		t.Error("readers started after failed discovery")
		return nil, nil
	}
	require.ErrorContains(t, multi.Run(nil), "discovery unavailable")
}

func TestMultiSourceFetchKeepsSequentialResultsAndErrors(t *testing.T) {
	for _, scenario := range []string{"success", "empty", "read_error", "create_error"} {
		t.Run(scenario, func(t *testing.T) {
			multi := newControlledMultiSource(t)
			multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
				if scenario == "empty" {
					return []LogbrokerInstance{"first.example:2135", "second.example:2135"}, nil
				}
				return []LogbrokerInstance{"first.example:2135"}, nil
			}
			var calls []LogbrokerInstance
			expectedError := xerrors.New("sample failed")
			multi.newSource = func(_ context.Context, cfg *LfSource, _ log.Logger, _ core_metrics.Registry) (abstract.Source, error) {
				calls = append(calls, cfg.Instance)
				if scenario == "create_error" {
					return nil, expectedError
				}
				source := &controlledSource{stopped: make(chan struct{})}
				source.fetch = func() ([]abstract.ChangeItem, error) {
					if cfg.Instance == "first.example:2135" {
						switch scenario {
						case "empty":
							return nil, nil
						case "read_error":
							return nil, expectedError
						}
					}
					return []abstract.ChangeItem{{Table: string(cfg.Instance)}}, nil
				}
				return source, nil
			}
			rows, err := multi.Fetch()
			if scenario == "read_error" || scenario == "create_error" {
				require.ErrorIs(t, err, expectedError)
			} else {
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, string(calls[len(calls)-1]), rows[0].Table)
			}
			if scenario == "empty" {
				require.Equal(t, []LogbrokerInstance{"first.example:2135", "second.example:2135"}, calls)
			} else {
				require.Equal(t, []LogbrokerInstance{"first.example:2135"}, calls)
			}
		})
	}
}

func TestMultiSourceStopCancelsPendingReaderStart(t *testing.T) {
	multi := newControlledMultiSource(t)
	multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
		return []LogbrokerInstance{"pending.example:2135"}, nil
	}
	started := make(chan struct{})
	multi.newSource = func(ctx context.Context, _ *LfSource, _ log.Logger, _ core_metrics.Registry) (abstract.Source, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- multi.Run(nil) }()
	<-started
	multi.Stop()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop pending connection attempts")
	}
}

func TestMultiSourceRetriesWithoutRediscovery(t *testing.T) {
	multi := newControlledMultiSource(t)
	resolveCalls := 0
	multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
		resolveCalls++
		return []LogbrokerInstance{"node.example:2135"}, nil
	}
	createCalls := 0
	expectedError := xerrors.New("reader failed")
	multi.newSource = func(context.Context, *LfSource, log.Logger, core_metrics.Registry) (abstract.Source, error) {
		createCalls++
		if createCalls == 1 {
			return nil, xerrors.New("temporary connection failure")
		}
		source := &controlledSource{stopped: make(chan struct{})}
		source.run = func() error {
			if createCalls == 2 {
				return mockTemporaryError(0)
			}
			return expectedError
		}
		return source, nil
	}
	require.ErrorIs(t, multi.Run(nil), expectedError)
	require.Equal(t, 3, createCalls)
	require.Equal(t, 1, resolveCalls)
}

func TestMultiSourceNewAttemptRediscoversInstances(t *testing.T) {
	var started []LogbrokerInstance
	for _, endpoint := range []LogbrokerInstance{"old.example:2135", "new.example:2135"} {
		multi := newControlledMultiSource(t)
		multi.resolve = func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error) {
			return []LogbrokerInstance{endpoint}, nil
		}
		multi.newSource = func(_ context.Context, cfg *LfSource, _ log.Logger, _ core_metrics.Registry) (abstract.Source, error) {
			started = append(started, cfg.Instance)
			return &controlledSource{stopped: make(chan struct{}), run: func() error { return xerrors.New("reader failed") }}, nil
		}
		require.Error(t, multi.Run(nil))
		require.Empty(t, multi.cfg.Instance)
	}
	require.Equal(t, []LogbrokerInstance{"old.example:2135", "new.example:2135"}, started)
}

func TestPersqueueTemporaryError(t *testing.T) {
	tempErrorCode, nonTempErrorCode := 0, 17
	var nonTempError = mockTemporaryError(nonTempErrorCode)
	var tempError = mockTemporaryError(tempErrorCode)
	require.False(t, isPersqueueTemporaryError(xerrors.New("irrelevant")), "irrelevant error")
	require.False(t, isPersqueueTemporaryError(nonTempError), "non-temporary")
	require.False(t, isPersqueueTemporaryError(xerrors.Errorf("oh: %w", nonTempError)), "wrapped non-temporary")
	require.True(t, isPersqueueTemporaryError(tempError), "temporary")
	require.True(t, isPersqueueTemporaryError(xerrors.Errorf("oh: %w", tempError)), "wrapped temporary")
}
