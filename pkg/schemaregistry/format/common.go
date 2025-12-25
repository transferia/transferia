package format

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// KafkaJSONSchemaFromArr convert KafkaJSONSchema as []byte to struct KafkaJSONSchema
func KafkaJSONSchemaFromArr(rawJSONSchema []byte) (*KafkaJSONSchema, error) {
	var kafkaSchema KafkaJSONSchema
	if err := json.Unmarshal(rawJSONSchema, &kafkaSchema); err != nil {
		return nil, xerrors.Errorf("Can't unmarshal schema into KafkaJSONSchema: %w", err)
	}
	return &kafkaSchema, nil
}
