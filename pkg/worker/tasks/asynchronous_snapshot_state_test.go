package tasks

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/cleanup"
	"github.com/transferria/transferria/pkg/middlewares/async/bufferer"
)

type fakeSink struct {
	push func(items []abstract.ChangeItem) error
}

func newFakeSink(push func(items []abstract.ChangeItem) error) *fakeSink {
	return &fakeSink{push: push}
}

func (s *fakeSink) Close() error {
	return nil
}

func (s *fakeSink) Push(items []abstract.ChangeItem) error {
	return s.push(items)
}

func TestAsynchronousSnapshotStateNonRowItem(t *testing.T) {
	sink := newFakeSink(func(items []abstract.ChangeItem) error {
		return errors.New("some error")
	})

	bufferer := bufferer.Bufferer(logger.Log, bufferer.BuffererConfig{TriggingCount: 0, TriggingSize: 0, TriggingInterval: 0}, solomon.NewRegistry(nil))
	asyncSink := bufferer(sink)
	defer cleanup.Close(asyncSink, logger.Log)

	state := newAsynchronousSnapshotState(asyncSink)
	pusher := state.SnapshotPusher()
	require.Error(t, pusher([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad},
	}))
	require.NoError(t, state.Close())
}
