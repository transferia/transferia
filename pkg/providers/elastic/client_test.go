//go:build !disable_elastic_provider

package elastic

import (
	"reflect"
	"testing"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/stretchr/testify/require"
)

func getProductCheckSuccessField(client *elasticsearch.Client) bool {
	return reflect.Indirect(reflect.ValueOf(&client).Elem()).FieldByName("productCheckSuccess").Bool()
}

func TestSetProductCheckSuccess(t *testing.T) {
	client := &elasticsearch.Client{}
	require.False(t, getProductCheckSuccessField(client))
	require.NoError(t, setProductCheckSuccess(client))
	require.True(t, getProductCheckSuccessField(client))
}
