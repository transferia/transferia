package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractNameClusterEngine(t *testing.T) {
	t.Run("", func(t *testing.T) {
		onClusterClause, engineStr, found := ExtractNameClusterEngine(
			"CREATE TABLE IF NOT EXISTS `test_table` (`id` Nullable(Int64), `payload` Nullable(String)) ENGINE=MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1",
		)
		require.Equal(t, "", onClusterClause)
		require.Equal(t, "MergeTree()", engineStr)
		require.Equal(t, true, found)
	})

	t.Run("", func(t *testing.T) {
		onClusterClause, engineStr, found := ExtractNameClusterEngine(
			"CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1- be74-91ee102b90b1'  ON CLUSTER `{cluster}`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		)
		require.Equal(t, "ON CLUSTER `{cluster}`", onClusterClause)
		require.Equal(t, "MergeTree", engineStr)
		require.Equal(t, true, found)
	})
}

func TestExtractEngine(t *testing.T) {
	t.Run("", func(t *testing.T) {
		engineStr, engineName, params, found := ExtractEngine(
			"MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1",
		)
		require.Equal(t, "MergeTree()", engineStr)
		require.Equal(t, "MergeTree", engineName)
		require.Equal(t, 0, len(params))
		require.Equal(t, true, found)
	})

	t.Run("", func(t *testing.T) {
		engineStr, engineName, params, found := ExtractEngine(
			"MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		)
		require.Equal(t, "MergeTree", engineStr)
		require.Equal(t, "MergeTree", engineName)
		require.Equal(t, 0, len(params))
		require.Equal(t, true, found)
	})

	t.Run("", func(t *testing.T) {
		engineStr, engineName, params, found := ExtractEngine(
			"ReplicatedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}')",
		)
		require.Equal(t, "ReplicatedMergeTree('/clickhouse/tables/{shard}/db/test_distr', '{replica}')", engineStr)
		require.Equal(t, "ReplicatedMergeTree", engineName)
		require.Equal(t, []string{"'/clickhouse/tables/{shard}/db/test_distr'", "'{replica}'"}, params)
		require.Equal(t, true, found)
	})
}
