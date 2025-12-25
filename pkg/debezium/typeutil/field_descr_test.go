package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func checkMysqlDatetime(t *testing.T, originalType, expectedDebeziumType, expectedName string) {
	colSchema := &abstract.ColSchema{
		OriginalType: originalType,
	}
	currType, name, additionalKV := TimestampMysqlParamsTypeToKafkaType(colSchema, false, false, debeziumparameters.EnrichedWithDefaults(nil))
	require.Equal(t, expectedDebeziumType, currType)
	require.Equal(t, expectedName, name)
	require.Nil(t, additionalKV)
}

func TestMysqlDatetime(t *testing.T) {
	checkMysqlDatetime(t, "mysql:datetime", "int64", "io.debezium.time.Timestamp")
	checkMysqlDatetime(t, "mysql:datetime(1)", "int64", "io.debezium.time.Timestamp")
	checkMysqlDatetime(t, "mysql:datetime(3)", "int64", "io.debezium.time.Timestamp")
	checkMysqlDatetime(t, "mysql:datetime(4)", "int64", "io.debezium.time.MicroTimestamp")
	checkMysqlDatetime(t, "mysql:datetime(6)", "int64", "io.debezium.time.MicroTimestamp")
}
