package serializer

import (
	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/xerrors"
)

type CsvBatchSerializerConfig struct {
	BatchConfig *BatchSerializerConfig
}

type csvBatchSerializer struct {
	serializer BatchSerializer
}

func NewCsvBatchSerializer(config *CsvBatchSerializerConfig) *csvBatchSerializer {
	c := config
	if c == nil {
		c = new(CsvBatchSerializerConfig)
	}

	return &csvBatchSerializer{
		serializer: newBatchSerializer(
			NewCsvSerializer(),
			nil,
			c.BatchConfig,
		),
	}
}

func (s *csvBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	data, err := s.serializer.Serialize(items)
	if err != nil {
		return nil, xerrors.Errorf("csvBatchSerializer: serialize: %w", err)
	}

	return data, nil
}
