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
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "fullfillment",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
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
	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/params__decimal.txt"))
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
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_"), "io.debezium.data.VariableScaleDecimal", "struct")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5_2"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_"), "io.debezium.data.VariableScaleDecimal", "struct")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5_2"), "org.apache.kafka.connect.data.Decimal", "bytes")
		checkNameAndType(t, afterSchema.FindSchemaDescr("money_"), "org.apache.kafka.connect.data.Decimal", "bytes")

		require.Equal(t, map[string]interface{}{"scale": json.Number("0"), "value": "EAAAAAAAAAAAAAAAAA=="}, getAfterVal(t, msg, "numeric_"))
		require.Equal(t, "MDk=", getAfterVal(t, msg, "numeric_5"))
		require.Equal(t, "ME8=", getAfterVal(t, msg, "numeric_5_2"))
		require.Equal(t, map[string]interface{}{"scale": json.Number("0"), "value": "AeJA"}, getAfterVal(t, msg, "decimal_"))
		require.Equal(t, "MDk=", getAfterVal(t, msg, "decimal_5"))
		require.Equal(t, "ME8=", getAfterVal(t, msg, "decimal_5_2"))
		require.Equal(t, "Jw4=", getAfterVal(t, msg, "money_"))
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
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5_2"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5_2"), "", "double")
		checkNameAndType(t, afterSchema.FindSchemaDescr("money_"), "", "double")

		require.Equal(t, json.Number("1.2676506002282294e+30"), getAfterVal(t, msg, "numeric_")) // should be: 1.2676506002282294E30
		require.Equal(t, json.Number("12345"), getAfterVal(t, msg, "numeric_5"))                 // should be: 12345.0
		require.Equal(t, json.Number("123.67"), getAfterVal(t, msg, "numeric_5_2"))
		require.Equal(t, json.Number("123456"), getAfterVal(t, msg, "decimal_")) // should be: 123456.0
		require.Equal(t, json.Number("12345"), getAfterVal(t, msg, "decimal_5")) // should be: 12345.0
		require.Equal(t, json.Number("123.67"), getAfterVal(t, msg, "decimal_5_2"))
		require.Equal(t, json.Number("99.98"), getAfterVal(t, msg, "money_"))
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
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("numeric_5_2"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("decimal_5_2"), "", "string")
		checkNameAndType(t, afterSchema.FindSchemaDescr("money_"), "", "string")

		require.Equal(t, "1267650600228229401496703205376", getAfterVal(t, msg, "numeric_"))
		require.Equal(t, "12345", getAfterVal(t, msg, "numeric_5"))
		require.Equal(t, "123.67", getAfterVal(t, msg, "numeric_5_2"))
		require.Equal(t, "123456", getAfterVal(t, msg, "decimal_"))
		require.Equal(t, "12345", getAfterVal(t, msg, "decimal_5"))
		require.Equal(t, "123.67", getAfterVal(t, msg, "decimal_5_2"))
		require.Equal(t, "99.98", getAfterVal(t, msg, "money_"))
	})
}
