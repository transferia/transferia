//go:build !disable_opensearch_provider

package opensearch

import (
	"os"
	"testing"

	"cuelang.org/go/pkg/regexp"
	"github.com/stretchr/testify/require"
)

func skip(t *testing.T) {
	t.SkipNow()
}

func TestCheckOpenSearchEqualElasticSearch(t *testing.T) {
	skip(t)

	openSearch, err := os.ReadFile("./model_opensearch_destination.go")
	require.NoError(t, err)

	elasticSearch, err := os.ReadFile("./model_elasticsearch_destination.go")
	require.NoError(t, err)

	openSearchExpected, err := regexp.ReplaceAll(`ElasticSearch`, string(elasticSearch), "OpenSearch")
	require.NoError(t, err)
	openSearchExpected, err = regexp.ReplaceAll(`ELASTICSEARCH`, openSearchExpected, "OPENSEARCH")
	require.NoError(t, err)
	openSearchExpected, err = regexp.ReplaceAll(`elasticsearch`, openSearchExpected, "opensearch")
	require.NoError(t, err)
	require.Equal(t, string(openSearch), openSearchExpected)
}
