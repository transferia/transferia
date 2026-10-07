package backoffutil

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestNewExponentialBackOff(t *testing.T) {
	backoff := NewExponentialBackOff()
	require.Equal(t, time.Duration(0), backoff.MaxElapsedTime)
}

func TestRetryNotifyFatal(t *testing.T) {
	fatal := abstract.NewFatalError(errors.New("fatal upload error"))
	wrapped := fmt.Errorf("upload part: %w", fatal)
	attempts, notifications := 0, 0
	err := RetryNotify(func() error {
		attempts++
		return wrapped
	}, WithMaxRetries(&backoff.ZeroBackOff{}, 3), func(error, time.Duration) {
		notifications++
	})
	require.Same(t, wrapped, err)
	require.ErrorIs(t, err, fatal)
	require.Equal(t, 1, attempts)
	require.Zero(t, notifications)
}

func TestRetryNotifyTransient(t *testing.T) {
	transient := errors.New("temporary upload error")
	attempts, notifications := 0, 0
	err := RetryNotify(func() error {
		attempts++
		if attempts < 3 {
			return transient
		}
		return nil
	}, WithMaxRetries(&backoff.ZeroBackOff{}, 3), func(err error, delay time.Duration) {
		require.ErrorIs(t, err, transient)
		require.Zero(t, delay)
		notifications++
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.Equal(t, 2, notifications)
}

func TestRetryNotifyMaxRetries(t *testing.T) {
	transient := errors.New("temporary upload error")
	attempts := 0
	err := RetryNotify(func() error {
		attempts++
		return transient
	}, WithMaxRetries(&backoff.ZeroBackOff{}, 3), nil)
	require.ErrorIs(t, err, transient)
	require.Equal(t, 4, attempts)
}
