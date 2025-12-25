package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewExponentialBackOff(t *testing.T) {
	backoff := NewExponentialBackOff()
	require.Equal(t, time.Duration(0), backoff.MaxElapsedTime)
}
