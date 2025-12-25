package format

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
)

var (
	//go:embed full_kafka_json_schema_test.json
	jsonKafkaSchema []byte

	//go:embed full_confluent_json_schema_test.json
	jsonConfluentSchema []byte

	//go:embed full_kafka_json_schema_arr_test.json
	jsonKafkaSchemaArrays []byte

	//go:embed full_confluent_json_schema_arr_test.json
	jsonConfluentSchemaArrays []byte
)

func TestMarshalUnmarshalKafka(t *testing.T) {
	t.Parallel()
	var kafkaSchema KafkaJSONSchema
	err := json.Unmarshal(jsonKafkaSchema, &kafkaSchema)
	require.NoError(t, err)
	out, err := json.Marshal(&kafkaSchema)
	require.NoError(t, err)
	require.JSONEq(t, string(jsonKafkaSchema), string(out))
}

func TestMarshalUnmarshalConfluent(t *testing.T) {
	t.Parallel()
	var confluent ConfluentJSONSchema
	err := json.Unmarshal(jsonConfluentSchema, &confluent)
	require.NoError(t, err)
	out, err := json.Marshal(&confluent)
	require.NoError(t, err)
	require.JSONEq(t, string(jsonConfluentSchema), string(out))

}

//func TestConfluentToKafkaToConfluent(t *testing.T) {
//	t.Parallel()
//	var confluent ConfluentJSONSchema
//	err := json.Unmarshal(jsonConfluentSchema, &confluent)
//	require.NoError(t, err)
//	kafka := confluent.ToKafkaJSONSchema()
//	confluent2 := kafka.ToConfluentSchema(false)
//	require.Equal(t, confluent, confluent2)
//}

func TestKafkaToConfluentToKafka(t *testing.T) {
	t.Parallel()
	var kafkaSchema KafkaJSONSchema
	err := json.Unmarshal(jsonKafkaSchema, &kafkaSchema)
	require.NoError(t, err)
	confluent := kafkaSchema.ToConfluentSchema(false)
	kafkaSchema2 := confluent.ToKafkaJSONSchema()
	require.Equal(t, kafkaSchema, kafkaSchema2)
}

func TestKafkaToConfluentClosedContentModel(t *testing.T) {
	t.Parallel()
	var kafkaSchema KafkaJSONSchema
	err := json.Unmarshal(jsonKafkaSchema, &kafkaSchema)
	require.NoError(t, err)
	confluent := kafkaSchema.ToConfluentSchema(true)
	// DEBUG
	confluentArr, _ := json.Marshal(confluent)
	fmt.Println("timmyb32rQQQ", string(confluentArr))
	// DEBUG
	//require.False(t, *confluent.AdditionalProperties)
}

func TestMarshalUnmarshalKafkaArrays(t *testing.T) {
	t.Parallel()
	var kafkaSchema KafkaJSONSchema
	err := json.Unmarshal(jsonKafkaSchemaArrays, &kafkaSchema)
	require.NoError(t, err)
	out, err := json.Marshal(&kafkaSchema)
	require.NoError(t, err)
	require.JSONEq(t, string(jsonKafkaSchemaArrays), string(out))
}

func TestMarshalUnmarshalConfluentArrays(t *testing.T) {
	t.Parallel()
	var confluent ConfluentJSONSchema
	err := json.Unmarshal(jsonConfluentSchemaArrays, &confluent)
	require.NoError(t, err)
	out, err := json.Marshal(&confluent)
	require.NoError(t, err)
	require.JSONEq(t, string(jsonConfluentSchemaArrays), string(out))

}

//func TestConfluentToKafkaToConfluentArrays(t *testing.T) {
//	t.Parallel()
//	var confluent ConfluentJSONSchema
//	err := json.Unmarshal(jsonConfluentSchemaArrays, &confluent)
//	require.NoError(t, err)
//	kafka := confluent.ToKafkaJSONSchema()
//	confluent2 := kafka.ToConfluentSchema(false)
//	require.Equal(t, confluent, confluent2)
//}

func TestKafkaToConfluentToKafkaArrays(t *testing.T) {
	t.Parallel()
	var kafkaSchema KafkaJSONSchema
	err := json.Unmarshal(jsonKafkaSchemaArrays, &kafkaSchema)
	require.NoError(t, err)
	confluent := kafkaSchema.ToConfluentSchema(false)
	kafkaSchema2 := confluent.ToKafkaJSONSchema()
	require.Equal(t, kafkaSchema, kafkaSchema2)
}

func TestCanonizeMakeClosedContentModelTrue(t *testing.T) {
	t.Parallel()
	var confluent ConfluentJSONSchema
	err := json.Unmarshal(jsonConfluentSchema, &confluent)
	require.NoError(t, err)
	kafka := confluent.ToKafkaJSONSchema()
	confluent2 := kafka.ToConfluentSchema(true)
	canon.SaveJSON(t, confluent2)
}
