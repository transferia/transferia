package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mapTypedValueTestKey string

func TestGetStringValueFromStringKeyMap(t *testing.T) {
	metadata := map[string]string{
		"resource": "config.yaml",
	}

	value, err := GetStringValue(metadata, "resource")
	require.NoError(t, err)
	require.Equal(t, "config.yaml", value)
}

func TestGetStringValueFromAliasKeyMap(t *testing.T) {
	transferParams := map[mapTypedValueTestKey]interface{}{
		mapTypedValueTestKey("transfer_id"): "tr-1",
	}

	value, err := GetStringValue(transferParams, mapTypedValueTestKey("transfer_id"))
	require.NoError(t, err)
	require.Equal(t, "tr-1", value)
}

func TestGetStringValueConvertsCompatibleKeyTypes(t *testing.T) {
	transferParams := map[mapTypedValueTestKey]interface{}{
		mapTypedValueTestKey("transfer_id"): "tr-1",
	}

	value, err := GetStringValue(transferParams, "transfer_id")
	require.NoError(t, err)
	require.Equal(t, "tr-1", value)
}

func TestGetBoolAndIntValue(t *testing.T) {
	values := map[string]interface{}{
		"enabled": true,
		"count":   3,
	}

	boolValue, err := GetBoolValue(values, "enabled")
	require.NoError(t, err)
	require.True(t, boolValue)

	intValue, err := GetIntValue(values, "count")
	require.NoError(t, err)
	require.Equal(t, 3, intValue)
}
