//go:build !disable_postgres_provider

package splitter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardingStorage_CalculatePartsCount(t *testing.T) {
	partCount := calculatePartCount(101, 100, 4)
	require.Equal(t, partCount, uint64(2))

	partCount = calculatePartCount(101, 100, 1)
	require.Equal(t, partCount, uint64(1))

	partCount = calculatePartCount(100, 100, 4)
	require.Equal(t, partCount, uint64(1))

	partCount = calculatePartCount(1001, 100, 4)
	require.Equal(t, partCount, uint64(4))
}
