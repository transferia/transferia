package yt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestParseTableIDForProvider_YT(t *testing.T) {
	t.Run("YT path as table name", func(t *testing.T) {
		test, err := abstract.ParseTableIDForProvider(`//home/market/production/mstat/analyst/regular/cubes_vertica/fact_delivery_plan`, ProviderType)
		require.NoError(t, err)
		require.Equal(t, abstract.TableID{Namespace: "", Name: "//home/market/production/mstat/analyst/regular/cubes_vertica/fact_delivery_plan"}, *test)
	})

	t.Run("YT path with multiple dots as table name", func(t *testing.T) {
		test, err := abstract.ParseTableIDForProvider(`//home/ads-analytics/projects/yan/cubes/yan/v4.0.0/cooked/tmp/2026-02-02`, ProviderType)
		require.NoError(t, err)
		require.Equal(t, abstract.TableID{Namespace: "", Name: "//home/ads-analytics/projects/yan/cubes/yan/v4.0.0/cooked/tmp/2026-02-02"}, *test)
	})
}
