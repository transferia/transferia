package dockercompose

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/elastic"
	"github.com/transferria/transferria/pkg/providers/opensearch"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	//go:embed data/elastic2opensearch/index.json
	elastic2opensearchIndexParams string
	//go:embed data/elastic2opensearch/data.json
	elastic2opensearchData string
	//go:embed data/elastic2opensearch/data_null.json
	elastic2opensearchDataNull string
)

func TestElasticToOpenSearchSnapshot(t *testing.T) {
	const elastic2opensearchTransferID = "elastic2opensearch"
	const srcPort = 9207
	const dstPort = 9200
	elasticSrc := elastic.ElasticSearchSource{
		ClusterID:            "",
		DataNodes:            []elastic.ElasticSearchHostPort{{Host: "localhost", Port: srcPort}},
		User:                 "user",
		Password:             "",
		SSLEnabled:           false,
		TLSFile:              "",
		SubNetworkID:         "",
		SecurityGroupIDs:     nil,
		DumpIndexWithMapping: true,
	}
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
	helpers.InitSrcDst(elastic2opensearchTransferID, &elasticSrc, &opensearchDst, abstract.TransferTypeSnapshotOnly)

	t.Parallel()

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Elastic source", Port: srcPort},
			helpers.LabeledPort{Label: "Opensearch target", Port: dstPort},
		))
	}()

	client := createTestElasticClientFromSrc(t, &elasticSrc)

	var indexName = "test_index_all_opensearch_types"
	createElasticIndex(t, client, indexName, elastic2opensearchIndexParams)
	time.Sleep(3 * time.Second)

	for i := 0; i < 5; i++ {
		pushElasticDoc(t, client, indexName, elastic2opensearchData, fmt.Sprint(i))
	}
	for i := 0; i < 5; i++ {
		pushElasticDoc(t, client, indexName, elastic2opensearchDataNull, fmt.Sprint(i+5))
	}
	_, err := elasticGetAllDocuments(client, indexName)
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(elastic2opensearchTransferID, &elasticSrc, &opensearchDst, abstract.TransferTypeSnapshotOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
	// dump data
	clientDst := createTestElasticClientFromDst(t, &opensearchDst)
	indexParams := dumpElasticIndexParams(t, clientDst, indexName)
	searchData, err := elasticGetAllDocuments(clientDst, indexName)
	require.NoError(t, err)

	logger.Log.Infof("%v", searchData)
	canon.SaveJSON(t, struct {
		IndexParams map[string]interface{}
		Data        interface{}
	}{
		IndexParams: indexParams,
		Data:        searchData,
	})
}
