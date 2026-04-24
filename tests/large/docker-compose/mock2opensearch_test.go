package dockercompose

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/opensearch"
	"github.com/transferia/transferia/tests/helpers"
)

func TestMockOpenSearchDataStreamSinkPush(t *testing.T) {
	const dstPort = 9200
	streamName := fmt.Sprintf("dsmock%x", time.Now().UnixNano())

	opensearchDst := opensearch.OpenSearchDestination{
		ClusterID:        "",
		DataNodes:        []opensearch.OpenSearchHostPort{{Host: "localhost", Port: dstPort}},
		User:             "user",
		Password:         "",
		SSLEnabled:       false,
		TLSFile:          "",
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		Cleanup:          model.Drop,
		SanitizeDocKeys:  false,
	}
	opensearchDst.WithDefaults()

	t.Parallel()

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "OpenSearch target", Port: dstPort},
		))
	}()

	client := createTestElasticClientFromDst(t, &opensearchDst)
	createOpenSearchDataStream(t, client, streamName)

	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "string", PrimaryKey: true},
		{ColumnName: "@timestamp", DataType: "datetime", PrimaryKey: false},
		{ColumnName: "payload", DataType: "string", PrimaryKey: false},
	})
	payload := "mock2opensearch-payload-" + streamName
	changeItem := abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       "",
		Table:        streamName,
		ColumnNames:  []string{"id", "@timestamp", "payload"},
		ColumnValues: []interface{}{"doc-1", "2026-04-21T12:00:00Z", payload},
		TableSchema:  tableSchema,
	}

	sink, err := opensearch.NewSink(&opensearchDst, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()

	err = sink.Push([]abstract.ChangeItem{changeItem})
	require.NoError(t, err)

	requireOpenSearchDataStreamIndex(t, client, streamName)

	docs, err := elasticGetAllDocuments(client, streamName)
	require.NoError(t, err)
	require.NotNil(t, docs)

	docsJSON, err := json.Marshal(docs)
	require.NoError(t, err)
	require.Contains(t, string(docsJSON), payload, "document with expected payload not found in data stream %q", streamName)
}

func createOpenSearchDataStream(t *testing.T, client *elasticsearch.Client, streamName string) {
	t.Helper()
	ctx := context.Background()

	templateBody := fmt.Sprintf(`{
  "index_patterns": [%q],
  "data_stream": {},
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "dynamic": true,
      "properties": {
        "@timestamp": { "type": "date" }
      }
    }
  }
}`, streamName)

	templateName := streamName + "-tmpl"
	res, err := client.Indices.PutIndexTemplate(templateName, strings.NewReader(templateBody),
		client.Indices.PutIndexTemplate.WithContext(ctx),
		client.Indices.PutIndexTemplate.WithMasterTimeout(30*time.Second),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	require.NoError(t, res.Body.Close())

	res, err = client.Indices.CreateDataStream(
		streamName,
		client.Indices.CreateDataStream.WithContext(ctx),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	require.NoError(t, res.Body.Close())
}

func requireOpenSearchDataStreamIndex(t *testing.T, client *elasticsearch.Client, indexName string) {
	t.Helper()
	ctx := context.Background()
	res, err := client.Indices.Get([]string{indexName}, client.Indices.Get.WithContext(ctx))
	require.NoError(t, err)
	defer res.Body.Close()
	require.False(t, res.IsError(), res.String())

	var parsed map[string]interface{}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&parsed))

	var backing map[string]interface{}
	for _, idxRaw := range parsed {
		idx, ok := idxRaw.(map[string]interface{})
		if !ok {
			continue
		}
		if ds, ok := idx["data_stream"].(string); ok && ds == indexName {
			backing = idx
			break
		}
	}
	require.NotNil(
		t,
		backing,
		"no backing index for data stream %q in GET response (expected metadata.data_stream=%q): %v",
		indexName,
		indexName,
		parsed,
	)
}
