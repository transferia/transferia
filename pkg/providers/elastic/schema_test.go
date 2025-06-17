//go:build !disable_elastic_provider

package elastic

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/tests/helpers/utils"
)

func TestFixDataTypesWithSampleData(t *testing.T) {
	storage, err := NewStorage(&ElasticSearchSource{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), ElasticSearch)
	require.NoError(t, err)
	searchFuncStub := func(o ...func(*esapi.SearchRequest)) (*esapi.Response, error) {
		readCloser := utils.NewTestReadCloser()
		readCloser.Add([]byte(`{"hits":{"hits":[{"_id":"my_id", "_source": {"k": null}}]}}`))
		return &esapi.Response{
			StatusCode: 200,
			Header:     nil,
			Body:       readCloser,
		}, nil
	}
	storage.Client.Search = searchFuncStub

	schemaDescription := &SchemaDescription{
		Columns: []abstract.ColSchema{
			{ColumnName: "k"},
		},
		ColumnsNames: []string{"k"},
	}

	err = storage.fixDataTypesWithSampleData("", schemaDescription)
	require.NoError(t, err)
}
