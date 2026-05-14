package s3_model

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/serializer"
)

type Encoding string

const (
	NoEncoding   = Encoding("UNCOMPRESSED")
	GzipEncoding = Encoding("GZIP")
	ZlibEncoding = Encoding("ZLIB")
)

type SerializerConfig interface {
	FormatName() model.ParsingFormat
	FormatEncoding() Encoding
	AsConfig() *serializer.BatchSerializerCommonConfig
}

var _ SerializerConfig = (*CSVSerializerConfig)(nil)
var _ SerializerConfig = (*JsonSerializerConfig)(nil)
var _ SerializerConfig = (*ParquetSerializerConfig)(nil)

// SerializerUnion holds at most one serializer variant (same idea as s3_model.Format on S3Source).
type SerializerUnion struct {
	Json    *JsonSerializerConfig
	CSV     *CSVSerializerConfig
	Parquet *ParquetSerializerConfig
}

type JsonSerializerConfig struct {
	AnyAsString bool
	Encoding    Encoding
}

func (s *JsonSerializerConfig) FormatName() model.ParsingFormat {
	return model.ParsingFormatJSON
}

func (s *JsonSerializerConfig) FormatEncoding() Encoding {
	return s.Encoding
}

func (s *JsonSerializerConfig) AsConfig() *serializer.BatchSerializerCommonConfig {
	return &serializer.BatchSerializerCommonConfig{
		UnsupportedItemKinds: nil,
		AddClosingNewLine:    true,
		Format:               s.FormatName(),
		AnyAsString:          s.AnyAsString,
		ParquetConfig:        nil,
	}
}

type CSVSerializerConfig struct {
	Encoding Encoding
}

func (s *CSVSerializerConfig) FormatName() model.ParsingFormat {
	return model.ParsingFormatCSV
}

func (s *CSVSerializerConfig) FormatEncoding() Encoding {
	return s.Encoding
}

func (s *CSVSerializerConfig) AsConfig() *serializer.BatchSerializerCommonConfig {
	return &serializer.BatchSerializerCommonConfig{
		UnsupportedItemKinds: nil,
		AddClosingNewLine:    true,
		Format:               s.FormatName(),
		AnyAsString:          false,
		ParquetConfig:        nil,
	}
}

type ParquetSerializerConfig struct {
	CompressionCodec string
	RowGroupMaxRows  int64
}

func (s *ParquetSerializerConfig) FormatName() model.ParsingFormat {
	return model.ParsingFormatPARQUET
}

func (s *ParquetSerializerConfig) FormatEncoding() Encoding {
	return NoEncoding
}

func (s *ParquetSerializerConfig) AsConfig() *serializer.BatchSerializerCommonConfig {
	return &serializer.BatchSerializerCommonConfig{
		UnsupportedItemKinds: nil,
		AddClosingNewLine:    true,
		Format:               s.FormatName(),
		AnyAsString:          false,
		ParquetConfig: &serializer.ParquetBatchSerializerConfig{
			CompressionCodec: serializer.CodecFromString(s.CompressionCodec),
			RowGroupMaxRows:  s.RowGroupMaxRows,
		},
	}
}
