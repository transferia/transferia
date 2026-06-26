package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildOriginalTypeForUserDefined(t *testing.T) {
	t.Run("enum", func(t *testing.T) {
		var domainName *string = nil
		require.Equal(t, "USER-DEFINED:ENUM:user_type_enum", buildOriginalTypeForUserDefined("user_type_enum", domainName, func() []string { return []string{"A", "B"} }))
	})
	t.Run("hstore", func(t *testing.T) {
		var domainName *string = nil
		require.Equal(t, "USER-DEFINED:hstore", buildOriginalTypeForUserDefined("hstore", domainName, func() []string { return nil }))
	})
	t.Run("citext", func(t *testing.T) {
		var domainName *string = nil
		require.Equal(t, "USER-DEFINED:citext", buildOriginalTypeForUserDefined("citext", domainName, func() []string { return nil }))
	})
	t.Run("composite", func(t *testing.T) {
		var domainName *string = nil
		require.Equal(t, "USER-DEFINED:COMPOSITE:my_type", buildOriginalTypeForUserDefined("my_type", domainName, func() []string { return nil }))
	})
}
