package mysql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestEnumParamsToKafkaType(t *testing.T) {
	colSchema := &abstract.ColSchema{
		OriginalType: "mysql:enum('x-small','small','medium','large','x-large')",
	}
	currType, name, kv := enumParamsToKafkaType(colSchema, false, true, nil)
	require.Equal(t, "string", currType)
	require.Equal(t, "io.debezium.data.Enum", name)
	kvStr, _ := json.Marshal(kv)
	require.Equal(t, `{"parameters":{"allowed":"x-small,small,medium,large,x-large"}}`, string(kvStr))
}

func TestSetParamsToKafkaType(t *testing.T) {
	colSchema := &abstract.ColSchema{
		OriginalType: "mysql:set('a','b','c','d')",
	}
	currType, name, kv := setParamsToKafkaType(colSchema, false, true, nil)
	require.Equal(t, "string", currType)
	require.Equal(t, "io.debezium.data.EnumSet", name)
	kvStr, _ := json.Marshal(kv)
	require.Equal(t, `{"parameters":{"allowed":"a,b,c,d"}}`, string(kvStr))
}
