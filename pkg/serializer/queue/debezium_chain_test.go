package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	debeziumengine "github.com/transferria/transferria/pkg/parsers/registry/debezium/engine"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
	confluentsrmock "github.com/transferria/transferria/tests/helpers/confluent_schema_registry_mock"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func getUpdateChangeItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind:   abstract.UpdateKind,
		Schema: "db_user",
		Table:  "coll",
		ColumnNames: []string{
			"id",
			"val",
		},
		ColumnValues: []interface{}{
			1,
			2,
		},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer", PrimaryKey: true},
			{ColumnName: "val", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer"},
		}),
		OldKeys: abstract.OldKeysType{
			KeyNames:  []string{"id"},
			KeyTypes:  nil,
			KeyValues: []interface{}{1},
		},
	}
}

func makeChainConversionWithSR(t *testing.T, changeItems []abstract.ChangeItem, paramsMap map[string]string) ([]abstract.ChangeItem, int) {
	schemaRegistryClient, err := confluent.NewSchemaRegistryClientWithTransport(debeziumparameters.GetValueConverterSchemaRegistryURL(paramsMap), "", logger.Log)
	require.NoError(t, err)
	parser := debeziumengine.NewDebeziumImpl(logger.Log, schemaRegistryClient, 1)

	dbzSerializer, err := NewDebeziumSerializer(paramsMap, true, true, false, logger.Log)
	require.NoError(t, err)

	serializedMessages, err := MultithreadingSerialize(dbzSerializer, nil, changeItems, 64, 64, debeziumparameters.GetBatchingMaxSize(paramsMap))
	require.NoError(t, err)

	result := make([]abstract.ChangeItem, 0)
	messagesCount := 0
	for _, serializedMessagesArr := range serializedMessages {
		for _, el := range serializedMessagesArr {
			recoveredChangeItems := parser.DoBuf(abstract.Partition{}, el.Value, 0, time.Time{})
			result = append(result, recoveredChangeItems...)
			messagesCount++
		}
	}
	return result, messagesCount
}

func try(t *testing.T, schemaRegistryMock *confluentsrmock.ConfluentSRMock, batchingMaxSize int) int {
	changeItem0 := getUpdateChangeItem()
	changeItem0.ColumnValues[0] = 111
	changeItem0.OldKeys.KeyValues[0] = 111
	changeItem1 := getUpdateChangeItem()
	changeItem1.ColumnValues[0] = 222
	changeItem1.OldKeys.KeyValues[0] = 222

	paramsMap := map[string]string{
		debeziumparameters.DatabaseDBName:                  "public",
		debeziumparameters.TopicPrefix:                     "my_topic",
		debeziumparameters.SourceType:                      "pg",
		debeziumparameters.ValueConverter:                  debeziumparameters.ConverterConfluentJSON,
		debeziumparameters.ValueSubjectNameStrategy:        debeziumparameters.SubjectRecordNameStrategy,
		debeziumparameters.ValueConverterSchemaRegistryURL: schemaRegistryMock.URL(),
		debeziumparameters.ValueConverterBasicAuthUserInfo: "my_user:my_pass",
		debeziumparameters.AddOriginalTypes:                debeziumparameters.BoolTrue,
	}

	if batchingMaxSize != 0 {
		paramsMap[debeziumparameters.BatchingMaxSize] = fmt.Sprintf("%d", batchingMaxSize)
	}

	recoveredChangeItems, dbzMsgCount := makeChainConversionWithSR(t, []abstract.ChangeItem{changeItem0, changeItem1}, paramsMap)

	require.Len(t, recoveredChangeItems, 2)
	require.Equal(t, int32(111), recoveredChangeItems[0].ColumnValues[0])
	require.Equal(t, int32(222), recoveredChangeItems[1].ColumnValues[0])

	return dbzMsgCount
}

func TestBatchingWithSR(t *testing.T) {
	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(nil, nil)
	defer schemaRegistryMock.Close()
	require.Equal(t, 2, try(t, schemaRegistryMock, 0))
	require.Equal(t, 1, try(t, schemaRegistryMock, 1048576))
}
