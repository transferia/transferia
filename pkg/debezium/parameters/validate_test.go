package parameters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	require.NoError(t, Validate(map[string]string{"": ""}, true))
	require.NoError(t, Validate(map[string]string{"": ""}, true))

	require.Error(t, Validate(map[string]string{"dt.batching.max.size": "1"}, true))
	require.Error(t, Validate(map[string]string{"dt.batching.max.size": "1"}, false))

	require.NoError(t, Validate(map[string]string{"dt.batching.max.size": "1", "value.converter.schema.registry.url": "localhost:80"}, true))
	require.Error(t, Validate(map[string]string{"dt.batching.max.size": "1", "value.converter.schema.registry.url": "localhost:80"}, false))

	require.NoError(t, Validate(map[string]string{"dt.batching.max.size": "1", "value.converter.ysr.namespace.id": "ns1"}, true))
	require.Error(t, Validate(map[string]string{"dt.batching.max.size": "1", "value.converter.ysr.namespace.id": "ns1"}, false))
}
