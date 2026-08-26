package debezium

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestSyntheticTypesPolicy(t *testing.T) {
	newParams := func(policy string) map[string]string {
		return debeziumparameters.EnrichedWithDefaults(map[string]string{
			debeziumparameters.SyntheticTypesPolicy: policy,
		})
	}
	addValue := func(params map[string]string, schema abstract.ColSchema, value any) (*debeziumcommon.Values, error) {
		result := debeziumcommon.NewValues(params)
		err := add(&schema, schema.ColumnName, value, schema.OriginalType, false, false, params, result)
		return result, err
	}

	t.Run("empty original type", func(t *testing.T) {
		schema := abstract.NewColSchema("lsn", ytschema.TypeUint64, false)

		for _, policy := range []string{"", debeziumparameters.SyntheticTypesPolicyFail} {
			params := debeziumparameters.EnrichedWithDefaults(nil)
			if policy != "" {
				params = newParams(policy)
			}
			_, err := addValue(params, schema, uint64(1))
			require.ErrorIs(t, err, errUnknownSource)
		}

		for _, value := range []uint64{0, 42, math.MaxInt64} {
			result, err := addValue(newParams(debeziumparameters.SyntheticTypesPolicyCommon), schema, value)
			require.NoError(t, err)
			require.Equal(t, value, result.V["lsn"])

			fieldDescr, err := getFieldDescr(schema, newParams(debeziumparameters.SyntheticTypesPolicyCommon), false, false)
			require.NoError(t, err)
			require.Equal(t, "int64", fieldDescr["type"])
		}
	})

	t.Run("source-specific types ignore policy", func(t *testing.T) {
		testCases := []struct {
			name         string
			originalType string
			value        any
		}{
			{name: "pg", originalType: "pg:integer", value: int32(42)},
			{name: "mysql", originalType: "mysql:int", value: int32(42)},
			{name: "ydb", originalType: "ydb:Int32", value: int32(42)},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				schema := abstract.MakeOriginallyTypedColSchema("value", ytschema.TypeInt32.String(), tc.originalType)
				var convertedValues []any
				for _, policy := range []string{debeziumparameters.SyntheticTypesPolicyFail, debeziumparameters.SyntheticTypesPolicyCommon} {
					result, err := addValue(newParams(policy), schema, tc.value)
					require.NoError(t, err)
					convertedValues = append(convertedValues, result.V["value"])
				}
				require.Equal(t, convertedValues[0], convertedValues[1])
			})
		}
	})

	t.Run("unknown non-empty original type", func(t *testing.T) {
		schema := abstract.MakeOriginallyTypedColSchema("value", ytschema.TypeInt32.String(), "unknown:int")
		_, err := addValue(newParams(debeziumparameters.SyntheticTypesPolicyCommon), schema, int32(42))
		require.ErrorIs(t, err, errUnknownSource)
	})

	t.Run("unsupported common type", func(t *testing.T) {
		schema := abstract.MakeOriginallyTypedColSchema("value", "unsupported", "")
		params := newParams(debeziumparameters.SyntheticTypesPolicyCommon)
		params[debeziumparameters.UnknownTypesPolicy] = debeziumparameters.UnknownTypesPolicySkip
		_, err := addValue(params, schema, "value")
		require.ErrorContains(t, err, "unable to convert synthetic event")
	})

	t.Run("supported common types", func(t *testing.T) {
		testCases := []struct {
			dataType ytschema.Type
			value    any
		}{
			{dataType: ytschema.TypeInt64, value: int64(42)},
			{dataType: ytschema.TypeInt32, value: int32(42)},
			{dataType: ytschema.TypeInt16, value: int16(42)},
			{dataType: ytschema.TypeInt8, value: int8(42)},
			{dataType: ytschema.TypeUint64, value: uint64(math.MaxInt64)},
			{dataType: ytschema.TypeUint32, value: uint32(42)},
			{dataType: ytschema.TypeUint16, value: uint16(42)},
			{dataType: ytschema.TypeUint8, value: uint8(42)},
			{dataType: ytschema.TypeFloat32, value: float32(42.5)},
			{dataType: ytschema.TypeFloat64, value: float64(42.5)},
			{dataType: ytschema.TypeBytes, value: []byte("value")},
			{dataType: ytschema.TypeString, value: "value"},
			{dataType: ytschema.TypeBoolean, value: true},
			{dataType: ytschema.TypeTimestamp, value: time.Unix(42, 0).UTC()},
			{dataType: ytschema.TypeAny, value: map[string]any{"key": "value"}},
		}
		params := newParams(debeziumparameters.SyntheticTypesPolicyCommon)
		for _, tc := range testCases {
			t.Run(tc.dataType.String(), func(t *testing.T) {
				schema := abstract.NewColSchema("value", tc.dataType, false)
				_, err := addValue(params, schema, tc.value)
				require.NoError(t, err)
				_, err = getFieldDescr(schema, params, false, false)
				require.NoError(t, err)
			})
		}
	})
}

func getKV(t *testing.T, changeItem *abstract.ChangeItem, addKeySchema, addValSchema bool) ([]byte, []byte) {
	params := map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	}
	params[debeziumparameters.KeyConverterSchemasEnable] = fmt.Sprintf("%v", addKeySchema)
	params[debeziumparameters.ValueConverterSchemasEnable] = fmt.Sprintf("%v", addValSchema)
	emitter, err := NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))
	return []byte(currDebeziumKV[0].DebeziumKey), []byte(*currDebeziumKV[0].DebeziumVal)
}

func containsSchema(t *testing.T, msg []byte) bool {
	type message struct {
		Schema interface{} `json:"schema"`
	}
	var msgVar message
	err := json.Unmarshal(msg, &msgVar)
	require.NoError(t, err)
	return msgVar.Schema != nil
}

func TestValueConverterOnOff(t *testing.T) {
	changeItem := &abstract.ChangeItem{Kind: abstract.InsertKind}
	k0, v0 := getKV(t, changeItem, true, true)
	require.True(t, containsSchema(t, k0))
	require.True(t, containsSchema(t, v0))
	require.True(t, strings.Contains(string(k0), `"payload"`)) // 'payload' level should present when schema is turned-on
	require.True(t, strings.Contains(string(v0), `"payload"`)) // 'payload' level should present when schema is turned-on
	k1, v1 := getKV(t, changeItem, false, false)
	require.False(t, containsSchema(t, k1))
	require.False(t, containsSchema(t, v1))
	require.False(t, strings.Contains(string(k1), `"payload"`)) // 'payload' level should absent when schema is turned-off
	require.False(t, strings.Contains(string(v1), `"payload"`)) // 'payload' level should absent when schema is turned-off
}

func TestEscapeHTMLMarshaling(t *testing.T) {
	changeItem := &abstract.ChangeItem{
		Kind: abstract.InsertKind,
		ColumnNames: []string{
			"id",
			"value",
		},
		ColumnValues: []interface{}{
			1,
			"<>!@#$%^&*()_",
		},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "value", DataType: ytschema.TypeString.String()},
		})}
	_, payload := getKV(t, changeItem, false, false)
	require.Contains(t, string(payload), `"value":"<>!@#$%^&*()_"`)
}

func TestTombstonesOnDelete(t *testing.T) {
	deleteItem := &abstract.ChangeItem{
		Kind:   abstract.DeleteKind,
		Schema: "public",
		Table:  "test",
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{TableSchema: "public", TableName: "test", ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true, OriginalType: "pg:integer"},
			{TableSchema: "public", TableName: "test", ColumnName: "data", DataType: ytschema.TypeFloat64.String(), OriginalType: "pg:numeric"},
		}),
		OldKeys: abstract.OldKeysType{KeyNames: []string{"id"}, KeyTypes: []string{"integer"}, KeyValues: []any{8}},
	}

	t.Run("TombstonesOnDelete-True", func(t *testing.T) {
		connectorParams := debeziumparameters.EnrichedWithDefaults(map[string]string{
			debeziumparameters.TombstonesOnDelete: debeziumparameters.BoolTrue,
		})
		emitter, err := NewMessagesEmitter(connectorParams, "1.0", false, logger.Log)
		require.NoError(t, err)
		messages, err := emitter.EmitKV(deleteItem, time.Now(), false, nil)
		require.NoError(t, err)
		nilValuesCounter := 0
		for _, msg := range messages {
			if msg.DebeziumVal == nil {
				nilValuesCounter++
			}
		}
		require.Equal(t, 1, nilValuesCounter)
	})

	t.Run("TombstonesOnDelete-False", func(t *testing.T) {
		connectorParams := debeziumparameters.EnrichedWithDefaults(map[string]string{
			debeziumparameters.TombstonesOnDelete: debeziumparameters.BoolFalse,
		})
		emitter, err := NewMessagesEmitter(connectorParams, "1.0", false, logger.Log)
		require.NoError(t, err)
		messages, err := emitter.EmitKV(deleteItem, time.Now(), false, nil)
		require.NoError(t, err)
		for _, msg := range messages {
			require.NotNil(t, msg.DebeziumVal)
		}
	})
}
