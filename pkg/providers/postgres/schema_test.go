package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithDisableReplicaIdentityFull(t *testing.T) {
	t.Run("default is false", func(t *testing.T) {
		extractor := NewSchemaExtractor()
		require.False(t, extractor.disableCheckReplIdentity)
	})

	t.Run("true", func(t *testing.T) {
		extractor := NewSchemaExtractor().WithDisableReplicaIdentityFull(true)
		require.True(t, extractor.disableCheckReplIdentity)
	})

	t.Run("false", func(t *testing.T) {
		extractor := NewSchemaExtractor().WithDisableReplicaIdentityFull(false)
		require.False(t, extractor.disableCheckReplIdentity)
	})

	t.Run("chaining", func(t *testing.T) {
		extractor := NewSchemaExtractor().
			WithDisableReplicaIdentityFull(true).
			WithExcludeViews(true).
			WithUseFakePrimaryKey(true)
		require.True(t, extractor.disableCheckReplIdentity)
		require.True(t, extractor.excludeViews)
		require.True(t, extractor.useFakePrimaryKey)
	})
}
