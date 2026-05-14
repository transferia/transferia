package serializer

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type BatchSerializerCommonConfig struct {
	Format               model.ParsingFormat
	UnsupportedItemKinds map[abstract.Kind]bool
	AddClosingNewLine    bool
	AnyAsString          bool
	ParquetConfig        *ParquetBatchSerializerConfig
}

func (c *BatchSerializerCommonConfig) toJSONConfig() *JSONSerializerConfig {
	return &JSONSerializerConfig{
		UnsupportedItemKinds: c.UnsupportedItemKinds,
		AddClosingNewLine:    c.AddClosingNewLine,
		AnyAsString:          c.AnyAsString,
	}
}

func (c *BatchSerializerCommonConfig) toRawConfig() *RawSerializerConfig {
	return &RawSerializerConfig{
		AddClosingNewLine: c.AddClosingNewLine,
	}
}

func NewBatchSerializer(config *BatchSerializerCommonConfig) BatchSerializer {
	c := config
	if c == nil {
		c = new(BatchSerializerCommonConfig)
	}

	var separator []byte
	if !c.AddClosingNewLine {
		separator = []byte("\n")
	}

	switch c.Format {
	case model.ParsingFormatRaw:
		return newBatchSerializer(
			NewRawSerializer(c.toRawConfig()),
			separator,
			nil,
		)
	case model.ParsingFormatJSON:
		return newBatchSerializer(
			NewJSONSerializer(c.toJSONConfig()),
			separator,
			nil,
		)
	case model.ParsingFormatCSV:
		return newBatchSerializer(
			NewCsvSerializer(),
			nil,
			nil,
		)
	case model.ParsingFormatPARQUET:
		return NewParquetBatchSerializer(c.ParquetConfig)
	}
	return nil
}
