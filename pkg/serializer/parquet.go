package serializer

import (
	"bytes"
	"context"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	parquet_gzip "github.com/parquet-go/parquet-go/compress/gzip"
	parquet_snappy "github.com/parquet-go/parquet-go/compress/snappy"
	parquet_zstd "github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/size"
	"github.com/transferia/transferia/pkg/util/slicesx"
)

const DefaultRowGroupMaxRows = 0
const DefaultRowGroupMaxBytes = 128 * uint64(size.MiB)

type ParquetBatchSerializerConfig struct {
	CompressionCodec compress.Codec
	RowGroupMaxRows  uint64
	RowGroupMaxBytes uint64
}

type parquetStreamSerializer struct {
	schema               *parquet.Schema
	compressionCodec     compress.Codec
	writer               *parquet.GenericWriter[struct{}]
	tableSchema          abstract.FastTableSchema
	rowGroupMaxRows      uint64
	rowGroupMaxBytes     uint64
	currentRowGroupRows  uint64
	currentRowGroupBytes uint64
}

var _ BatchSerializer = (*parquetBatchSerializer)(nil)

// works via stream serializer
type parquetBatchSerializer struct {
	schema           *parquet.Schema
	compressionCodec compress.Codec
	tableSchema      abstract.FastTableSchema
	streamSerializer *parquetStreamSerializer
	rowGroupMaxRows  uint64
	rowGroupMaxBytes uint64

	buffer *bytes.Buffer
}

func (s *parquetBatchSerializer) SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
	serialized, err := s.Serialize(items)
	if err != nil {
		return 0, xerrors.Errorf("ParquetBatchSerialize: unable to serialize items: %w", err)
	}
	written, err := writer.Write(serialized)
	if err != nil {
		return 0, xerrors.Errorf("ParquetBatchSerialize: unable to write data: %w", err)
	}

	return written, nil
}

func (s *parquetBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	if s.schema == nil {
		s.buffer = bytes.NewBuffer(make([]byte, 0))
		parquetSchema, err := BuildParquetSchema(items[0].TableSchema.FastColumns())
		if err != nil {
			return nil, xerrors.Errorf("s3_sink: failed to create serializer: %w", err)
		}
		s.schema = parquetSchema
		s.tableSchema = items[0].TableSchema.FastColumns()

		s.streamSerializer, err = NewParquetStreamSerializer(s.buffer, s.schema, s.tableSchema, s.compressionCodec, s.rowGroupMaxRows, s.rowGroupMaxBytes)
		if err != nil {
			return nil, xerrors.Errorf("ParquetBatchSerialize: unable to build underlying stream serializer: %w", err)
		}
	}
	if err := s.streamSerializer.Serialize(items); err != nil {
		return nil, xerrors.Errorf("ParquetBatchSerialize: unable to serialize items: %w", err)
	}

	serialized := s.buffer.Bytes()
	s.buffer.Reset()

	return serialized, nil
}

func (s *parquetBatchSerializer) Close() ([]byte, error) {
	if err := s.streamSerializer.Close(); err != nil {
		return nil, xerrors.Errorf("ParquetBatchSerialize: unable to close stream serializer: %w", err)
	}
	return s.buffer.Bytes(), nil
}

func (s *parquetStreamSerializer) SetStream(ostream io.Writer) error {
	if err := s.Close(); err != nil {
		return xerrors.Errorf("parquetStreamSerializer: failed to close sink: %w", err)
	}

	options := []parquet.WriterOption{
		parquet.Compression(s.compressionCodec),
		s.schema,
	}
	s.writer = parquet.NewGenericWriter[struct{}](ostream, options...)
	s.currentRowGroupRows = 0
	s.currentRowGroupBytes = 0

	return nil
}

func (s *parquetStreamSerializer) Serialize(items []*abstract.ChangeItem) (err error) {
	if s.writer == nil {
		return xerrors.New("ParquetStreamSerializer: attempt to serialize with closed writer")
	}
	// this shitty lib use a lot of panic instead of err
	defer func() {
		if r := recover(); r != nil {
			err = xerrors.Errorf("was panic, recovered value: %v", r)
		}
	}()

	parts, size, length := slicesx.SplitByBytesOrLength(items, func(item *abstract.ChangeItem) uint64 {
		return item.Size.Values
	}, s.currentRowGroupBytes, s.currentRowGroupRows, s.rowGroupMaxBytes, s.rowGroupMaxRows)
	s.currentRowGroupBytes = size
	s.currentRowGroupRows = length

	for partIdx, part := range parts {
		rows, err := toParquetRows(part, s.schema, s.tableSchema)
		if err != nil {
			return xerrors.Errorf("ParquetStreamSerializer: failed to convert items to rows: %w", err)
		}
		if _, err := s.writer.WriteRows(rows); err != nil {
			return xerrors.Errorf("ParquetStreamSerializer: failed to write rows: %w", err)
		}
		if partIdx != len(parts)-1 {
			if err := s.writer.Flush(); err != nil {
				return xerrors.Errorf("ParquetStreamSerializer: failed to flush row group: %w", err)
			}
		}
	}

	return nil
}

func (s *parquetStreamSerializer) Close() (err error) {
	if s.writer != nil {
		defer func() {
			if r := recover(); r != nil {
				err = xerrors.Errorf("unable to close serializer: %v", r)
			}
		}()
		if err := s.writer.Close(); err != nil {
			return xerrors.Errorf("ParquetStreamSerializer: failed to close stream, err: %w", err)
		}
	}
	return err
}

// CodecFromString maps a parquet compression codec name to the corresponding compress.Codec.
// Returns nil (uncompressed) for empty string or "UNCOMPRESSED".
func CodecFromString(codec string) compress.Codec {
	switch codec {
	case "SNAPPY":
		return &parquet_snappy.Codec{}
	case "GZIP":
		return &parquet_gzip.Codec{}
	case "ZSTD":
		return &parquet_zstd.Codec{}
	default:
		return nil
	}
}

func NewParquetStreamSerializer(ostream io.Writer, schema *parquet.Schema, tableSchema abstract.FastTableSchema, compressionCodec compress.Codec, rowGroupMaxRows uint64, rowGroupMaxBytes uint64) (*parquetStreamSerializer, error) {
	pqSerializer := parquetStreamSerializer{
		schema:               schema,
		writer:               nil,
		tableSchema:          tableSchema,
		compressionCodec:     compressionCodec,
		rowGroupMaxRows:      rowGroupMaxRows,
		rowGroupMaxBytes:     rowGroupMaxBytes,
		currentRowGroupRows:  0,
		currentRowGroupBytes: 0,
	}

	err := pqSerializer.SetStream(ostream)

	if err != nil {
		return nil, err
	}
	return &pqSerializer, nil
}

func NewParquetBatchSerializer(config *ParquetBatchSerializerConfig) BatchSerializer {
	rowGroupMaxBytes := config.RowGroupMaxBytes
	if config.RowGroupMaxRows == 0 && config.RowGroupMaxBytes == 0 {
		rowGroupMaxBytes = DefaultRowGroupMaxBytes
	}
	return NewStrictifyingBatchSerializer(&parquetBatchSerializer{
		schema:           nil,
		tableSchema:      nil,
		compressionCodec: config.CompressionCodec,
		streamSerializer: nil,
		buffer:           nil,
		rowGroupMaxRows:  config.RowGroupMaxRows,
		rowGroupMaxBytes: rowGroupMaxBytes,
	})
}
