package serializer

import (
	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/xerrors"
)

type RawBatchSerializerConfig struct {
	SerializerConfig *RawSerializerConfig
	BatchConfig      *BatchSerializerConfig
}

type rawBatchSerializer struct {
	serializer BatchSerializer
}

func NewRawBatchSerializer(config *RawBatchSerializerConfig) *rawBatchSerializer {
	c := config
	if c == nil {
		c = new(RawBatchSerializerConfig)
	}

	var separator []byte
	if c.SerializerConfig == nil || !c.SerializerConfig.AddClosingNewLine {
		separator = []byte("\n")
	}

	return &rawBatchSerializer{
		serializer: newBatchSerializer(
			NewRawSerializer(c.SerializerConfig),
			separator,
			c.BatchConfig,
		),
	}
}

func (s *rawBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	data, err := s.serializer.Serialize(items)
	if err != nil {
		return nil, xerrors.Errorf("rawBatchSerializer: serialize: %w", err)
	}

	return data, nil
}
