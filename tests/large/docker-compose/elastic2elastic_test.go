package dockercompose

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/elastic"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	//go:embed data/elastic2elastic/index.json
	elastic2elasticIndexParams string
	//go:embed data/elastic2elastic/data.json
	elastic2elasticData string
	//go:embed data/elastic2elastic/data_null.json
	elastic2elasticDataNull string
)

func TestElasticToElasticSnapshot(t *testing.T) {
	const elastic2elasticTransferID = "elastic2elastic"
	const srcPort = 9205
	const dstPort = 9206
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
	elasticDst := elastic.ElasticSearchDestination{
		ClusterID:        "",
		DataNodes:        []elastic.ElasticSearchHostPort{{Host: "localhost", Port: dstPort}},
		User:             "user",
		Password:         "",
		SSLEnabled:       false,
		TLSFile:          "",
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		Cleanup:          model.Drop,
		SanitizeDocKeys:  false,
	}
	helpers.InitSrcDst(elastic2elasticTransferID, &elasticSrc, &elasticDst, abstract.TransferTypeSnapshotOnly)

	t.Parallel()

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Elastic source", Port: srcPort},
			helpers.LabeledPort{Label: "Elastic target", Port: dstPort},
		))
	}()
	client := createTestElasticClientFromSrc(t, &elasticSrc)

	var indexName = "test_index_all_elastic_types"
	createElasticIndex(t, client, indexName, elastic2elasticIndexParams)
	time.Sleep(3 * time.Second)

	for i := 0; i < 5; i++ {
		pushElasticDoc(t, client, indexName, elastic2elasticData, fmt.Sprint(i))
	}
	for i := 0; i < 5; i++ {
		pushElasticDoc(t, client, indexName, elastic2elasticDataNull, fmt.Sprint(i+5))
	}
	_, err := elasticGetAllDocuments(client, indexName)
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(elastic2elasticTransferID, &elasticSrc, &elasticDst, abstract.TransferTypeSnapshotOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	clientDst := createTestElasticClientFromDst(t, &elasticDst)

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
