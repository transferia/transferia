package tests

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func getParams(additional map[string]string) map[string]string {
	result := map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "mysql",
	}
	for k, v := range additional {
		result[k] = v
	}
	return result
}

func getAfterVal(t *testing.T, msg *debeziumcommon.Message, fieldName string) interface{} {
	for k, v := range msg.Payload.After {
		if k == fieldName {
			return v
		}
	}
	require.Fail(t, "unable to find field")
	return nil
}

func checkNameAndType(t *testing.T, schema *debeziumcommon.Schema, expectedName, expectedType string) {
	require.Equal(t, expectedName, schema.Name)
	require.Equal(t, expectedType, schema.Type)
}

func TestDecimalHandlingMode(t *testing.T) {
	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/params__decimal.txt"))
	require.NoError(t, err)
	changeItem, err := abstract.UnmarshalChangeItem(changeItemStr)
	require.NoError(t, err)

	checkPrecise := func(t *testing.T, additionalParams map[string]string) {
		emitter, err := debezium.NewMessagesEmitter(getParams(additionalParams), "1.1.2.Final", false, logger.Log)
		require.NoError(t, err)
		currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(currDebeziumKV))
		msg, err := debeziumcommon.UnmarshalMessage(*currDebeziumKV[0].DebeziumVal)
		require.NoError(t, err)

		afterSchema := msg.Schema.FindAfterSchema()
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5_2"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5_2"), "org.apache.kafka.connect.data.Decimal", "bytes")

		require.Equal(t, "SZYC0g==", getAfterVal(t, msg, "NUMERIC_"))
		require.Equal(t, "MDk=", getAfterVal(t, msg, "NUMERIC_5"))
		require.Equal(t, "MDk=", getAfterVal(t, msg, "NUMERIC_5_2"))
		require.Equal(t, "AIvQODU=", getAfterVal(t, msg, "DECIMAL_"))
		require.Equal(t, "W5s=", getAfterVal(t, msg, "DECIMAL_5"))
		require.Equal(t, "Wmk=", getAfterVal(t, msg, "DECIMAL_5_2"))
	}

	//---

	t.Run("decimal.handling.mode=precise(implicit,default)", func(t *testing.T) {
		checkPrecise(t, nil)
	})

	t.Run("decimal.handling.mode=precise(explicit)", func(t *testing.T) {
		checkPrecise(t, map[string]string{debeziumparameters.DecimalHandlingMode: "precise"})
	})

	t.Run("decimal.handling.mode=double", func(t *testing.T) {
		emitter, err := debezium.NewMessagesEmitter(getParams(map[string]string{debeziumparameters.DecimalHandlingMode: "double"}), "1.1.2.Final", false, logger.Log)
		require.NoError(t, err)
		currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(currDebeziumKV))
		msg, err := debeziumcommon.UnmarshalMessage(*currDebeziumKV[0].DebeziumVal)
		require.NoError(t, err)

		afterSchema := msg.Schema.FindAfterSchema()
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5_2"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5_2"), "", "double")

		require.Equal(t, json.Number("1234567890"), getAfterVal(t, msg, "NUMERIC_")) // should be: 1.23456789E9
		require.Equal(t, json.Number("12345"), getAfterVal(t, msg, "NUMERIC_5"))     // should be: 12345.0
		require.Equal(t, json.Number("123.45"), getAfterVal(t, msg, "NUMERIC_5_2"))
		require.Equal(t, json.Number("2345678901"), getAfterVal(t, msg, "DECIMAL_")) // should be: 2.345678901E9
		require.Equal(t, json.Number("23451"), getAfterVal(t, msg, "DECIMAL_5"))     // should be: 23451.0
		require.Equal(t, json.Number("231.45"), getAfterVal(t, msg, "DECIMAL_5_2"))
	})

	t.Run("decimal.handling.mode=string", func(t *testing.T) {
		emitter, err := debezium.NewMessagesEmitter(getParams(map[string]string{debeziumparameters.DecimalHandlingMode: "string"}), "1.1.2.Final", false, logger.Log)
		require.NoError(t, err)
		currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(currDebeziumKV))
		msg, err := debeziumcommon.UnmarshalMessage(*currDebeziumKV[0].DebeziumVal)
		require.NoError(t, err)

		afterSchema := msg.Schema.FindAfterSchema()
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("NUMERIC_5_2"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("DECIMAL_5_2"), "", "string")

		require.Equal(t, "1234567890", getAfterVal(t, msg, "NUMERIC_"))
		require.Equal(t, "12345", getAfterVal(t, msg, "NUMERIC_5"))
		require.Equal(t, "123.45", getAfterVal(t, msg, "NUMERIC_5_2"))
		require.Equal(t, "2345678901", getAfterVal(t, msg, "DECIMAL_"))
		require.Equal(t, "23451", getAfterVal(t, msg, "DECIMAL_5"))
		require.Equal(t, "231.45", getAfterVal(t, msg, "DECIMAL_5_2"))
	})
}
