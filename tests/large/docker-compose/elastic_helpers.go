package dockercompose

import (
	"encoding/json"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/elastic"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"golang.org/x/xerrors"
)

func createElasticIndex(t *testing.T, esClient *elasticsearch.Client, indexName string, indexParamsRawJSON string) {
	res, err := esClient.Indices.Create(indexName,
		esClient.Indices.Create.WithMasterTimeout(time.Second*30),
		esClient.Indices.Create.WithBody(strings.NewReader(indexParamsRawJSON)),
	)
	require.NoError(t, err)
	err = elastic.WaitForIndexToExist(esClient, indexName, time.Second*30)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	_, err = elasticGetAllDocuments(esClient, indexName)
	require.NoError(t, err)
}

func pushElasticDoc(t *testing.T, esClient *elasticsearch.Client, indexName string, docRawJSON string, id string) {
	res, err := esClient.Index(
		indexName,
		strings.NewReader(docRawJSON),
		esClient.Index.WithDocumentID(id),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
}

func dumpElasticIndexParams(t *testing.T, esClient *elasticsearch.Client, indexName string) map[string]interface{} {
	resp, err := esClient.Indices.Get([]string{indexName})
	require.NoError(t, err)
	require.False(t, resp.IsError())
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var ans map[string]interface{}
	require.NoError(t, json.Unmarshal(body, &ans))
	indexParams, ok := ans[indexName]
	require.True(t, ok)
	asMap, ok := indexParams.(map[string]interface{})
	require.True(t, ok)

	elastic.DeleteSystemFieldsFromIndexParams(asMap)

	return asMap
}

func createTestElasticClientFromSrc(t *testing.T, elasticLike elastic.IsElasticLikeSource) *elasticsearch.Client {
	src, serverType := elasticLike.ToElasticSearchSource()
	dst := src.SourceToElasticSearchDestination()
	config, err := elastic.ConfigFromDestination(logger.Log, dst, serverType)
	require.NoError(t, err)
	client, err := elastic.WithLogger(*config, logger.Log, serverType)
	require.NoError(t, err)
	return client
}

func createTestElasticClientFromDst(t *testing.T, elasticLike elastic.IsElasticLikeDestination) *elasticsearch.Client {
	dst, serverType := elasticLike.ToElasticSearchDestination()
	config, err := elastic.ConfigFromDestination(logger.Log, dst, serverType)
	require.NoError(t, err)
	client, err := elastic.WithLogger(*config, logger.Log, serverType)
	require.NoError(t, err)
	return client
}

func elasticGetAllDocuments(esClient *elasticsearch.Client, indexes ...string) (interface{}, error) {
	// Wait for data
	// (https://stackoverflow.com/questions/40676324/elasticsearch-updates-are-not-immediate-how-do-you-wait-for-elasticsearch-to-fi)

	_, err := esClient.Indices.Refresh(
		esClient.Indices.Refresh.WithIndex(indexes...))
	if err != nil {
		return "", xerrors.Errorf("elastic refresh error: %w", err)
	}

	_, err = esClient.Cluster.Health(
		esClient.Cluster.Health.WithWaitForNoRelocatingShards(true),
		esClient.Cluster.Health.WithWaitForActiveShards("all"))
	if err != nil {
		return "", xerrors.Errorf("elastic health error: %w", err)
	}

	// Get data

	searchResponse, err := esClient.Search(
		esClient.Search.WithSize(10000),
		esClient.Search.WithIndex(indexes...))
	if err != nil {
		return "", xerrors.Errorf("elastic search error: %w", err)
	}
	var searchResponseData struct {
		Hits struct {
			Hits []struct {
				Index  string      `json:"_index"`
				Type   string      `json:"_type"`
				ID     string      `json:"_id"`
				Source interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	err = jsonx.NewDefaultDecoder(searchResponse.Body).Decode(&searchResponseData)
	if err != nil {
		return "", xerrors.Errorf("can't decode elastic stat response: %w", err)
	}
	hits := searchResponseData.Hits.Hits
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].ID > hits[j].ID
	})
	return hits, nil
}
