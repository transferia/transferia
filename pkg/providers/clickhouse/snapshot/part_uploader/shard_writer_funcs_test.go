package part_uploader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotNativeIOTimeoutStopsBeforeCancellation(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopTimeoutAndWait := startIOTimeout(time.Hour, cancel)
	stopTimeoutAndWait()
	require.NoError(test, ctx.Err())
}

func TestSnapshotNativeIOTimeoutWaitsForCancellation(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{}, 1)
	defer close(releaseCancel)

	stopTimeoutAndWait := startIOTimeout(0, func() {
		close(cancelStarted)
		<-releaseCancel
		cancel()
	},
	)
	select {
	case <-cancelStarted:
	case <-time.After(time.Second):
		test.Fatal("timeout cancellation did not start")
	}

	stopped := make(chan struct{})
	go func() {
		stopTimeoutAndWait()
		close(stopped)
	}()
	select {
	case <-stopped:
		test.Fatal("timeout stop returned before cancellation completed")
	case <-time.After(20 * time.Millisecond):
	}
	releaseCancel <- struct{}{}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		test.Fatal("timeout stop did not wait for cancellation")
	}
	require.ErrorIs(test, ctx.Err(), context.Canceled)
}
