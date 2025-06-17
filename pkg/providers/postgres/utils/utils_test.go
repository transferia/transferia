//go:build !disable_postgres_provider

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleHostAndHosts(t *testing.T) {
	// intersection cases
	t.Run("intersection case - host duplicated 1-to-1", func(t *testing.T) {
		result := HandleHostAndHosts("A", []string{"A"})
		require.Len(t, result, 1)
		require.Equal(t, "A", result[0])
	})
	t.Run("intersection case - host duplicated 1-to-many, duplicate at 1st place", func(t *testing.T) {
		result := HandleHostAndHosts("A", []string{"A", "B"})
		require.Len(t, result, 2)
		require.Equal(t, "A", result[0])
		require.Equal(t, "B", result[1])
	})
	t.Run("intersection case - host duplicated 1-to-many, duplicate not at 1st place", func(t *testing.T) {
		result := HandleHostAndHosts("A", []string{"B", "A"})
		require.Len(t, result, 2)
		require.Equal(t, "A", result[0])
		require.Equal(t, "B", result[1])
	})
	t.Run("intersection case - host differs from hosts", func(t *testing.T) {
		result := HandleHostAndHosts("A", []string{"B"})
		require.Len(t, result, 2)
		require.Equal(t, "A", result[0])
		require.Equal(t, "B", result[1])
	})

	// mutual exclusive cases
	t.Run("mutual exclusive case - there are only 'host'", func(t *testing.T) {
		result := HandleHostAndHosts("A", []string{})
		require.Len(t, result, 1)
		require.Equal(t, "A", result[0])
	})
	t.Run("mutual exclusive case - there are only 'hosts'", func(t *testing.T) {
		result := HandleHostAndHosts("", []string{"B"})
		require.Len(t, result, 1)
		require.Equal(t, "B", result[0])
	})

	// invalid cases
	t.Run("invalid case - both fields: 'host' & 'hosts' not filled, hosts=nil", func(t *testing.T) {
		result := HandleHostAndHosts("", nil)
		require.Nil(t, result)
	})
	t.Run("invalid case - both fields: 'host' & 'hosts' not filled, hosts=[]string{}", func(t *testing.T) {
		result := HandleHostAndHosts("", []string{})
		require.Nil(t, result)
	})
	t.Run("invalid case - 'hosts' has an empty string", func(t *testing.T) {
		result := HandleHostAndHosts("", []string{"A", ""})
		require.Len(t, result, 1)
		require.Equal(t, "A", result[0])
	})
}
