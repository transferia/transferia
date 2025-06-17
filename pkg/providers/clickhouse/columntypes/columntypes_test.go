//go:build !disable_clickhouse_provider

package columntypes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumntPrecision(t *testing.T) {
	require.Equal(t, NewTypeDescription("DateTime64(1)").DateTime64Precision(), 1)
	require.Equal(t, NewTypeDescription("DateTime64(2)").DateTime64Precision(), 2)
	require.Equal(t, NewTypeDescription("DateTime64(3)").DateTime64Precision(), 3)
	require.Equal(t, NewTypeDescription("DateTime64(4)").DateTime64Precision(), 4)
	require.Equal(t, NewTypeDescription("DateTime64(5)").DateTime64Precision(), 5)
	require.Equal(t, NewTypeDescription("DateTime64(6)").DateTime64Precision(), 6)
	require.Equal(t, NewTypeDescription("DateTime64").DateTime64Precision(), 0)
	require.Equal(t, NewTypeDescription("DateTime64(6, 'Asia/Istanbul')").DateTime64Precision(), 6)
	require.Equal(t, NewTypeDescription("Nullable(DateTime64(5, 'Asia/Istanbul'))").DateTime64Precision(), 5)
}
