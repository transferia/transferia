package serializer

import (
	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/xerrors"
)

type JSONBatchSerializerConfig struct {
	SerializerConfig *JSONSerializerConfig
	BatchConfig      *BatchSerializerConfig
}

type jsonBatchSerializer struct {
	serializer BatchSerializer
}

func NewJSONBatchSerializer(config *JSONBatchSerializerConfig) *jsonBatchSerializer {
	c := config
	if c == nil {
		c = new(JSONBatchSerializerConfig)
	}

	var separator []byte
	if c.SerializerConfig == nil || !c.SerializerConfig.AddClosingNewLine {
		separator = []byte("\n")
	}

	return &jsonBatchSerializer{
		serializer: newBatchSerializer(
			NewJSONSerializer(c.SerializerConfig),
			separator,
			c.BatchConfig,
		),
	}
}

func (s *jsonBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	data, err := s.serializer.Serialize(items)
	if err != nil {
		return nil, xerrors.Errorf("jsonBatchSerializer: serialize: %w", err)
	}

	return data, nil
}
