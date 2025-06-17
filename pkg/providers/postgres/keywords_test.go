//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsKeyword(t *testing.T) {
	require.True(t, IsKeyword("boolean"))
	require.True(t, IsKeyword("BOOLEAN"))
	require.True(t, IsKeyword("INTEGER"))
	require.True(t, IsKeyword("INTEger"))
	require.False(t, IsKeyword("zhopa"))
	require.False(t, IsKeyword("ZHOPA"))
}
