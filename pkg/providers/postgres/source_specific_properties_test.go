//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestEnumAllValues(t *testing.T) {
	currColSchema := &abstract.ColSchema{
		Properties: map[abstract.PropertyKey]interface{}{EnumAllValues: []string{"a", "b"}},
	}
	arr := GetPropertyEnumAllValues(currColSchema)
	require.NotNil(t, arr)
	require.Len(t, arr, 2)
}
