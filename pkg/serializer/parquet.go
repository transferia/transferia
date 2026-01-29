package serializer

import (
	"bytes"
	"context"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type parquetStreamSerializer struct {
	schema           *parquet.Schema
	compressionCodec compress.Codec
	writer           *parquet.GenericWriter[struct{}]
	tableSchema      abstract.FastTableSchema
}

var _ BatchSerializer = (*parquetBatchSerializer)(nil)

// works via stream serializer
type parquetBatchSerializer struct {
	schema           *parquet.Schema
	compressionCodec compress.Codec
	tableSchema      abstract.FastTableSchema
	streamSerializer *parquetStreamSerializer

	buffer *bytes.Buffer
}

func (s *parquetBatchSerializer) SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) error {
	serialized, err := s.Serialize(items)
	if err != nil {
		return xerrors.Errorf("ParquetBatchSerialize: unable to serialize items: %w", err)
	}
	if _, err := writer.Write(serialized); err != nil {
		return xerrors.Errorf("ParquetBatchSerialize: unable to write data: %w", err)
	}
	return nil
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

		s.streamSerializer, err = NewParquetStreamSerializer(s.buffer, s.schema, s.tableSchema, s.compressionCodec)
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

	options := []parquet.WriterOption{parquet.Compression(s.compressionCodec), s.schema}
	s.writer = parquet.NewGenericWriter[struct{}](ostream, options...)

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
	rows := make([]parquet.Row, len(items))

	for i, item := range items {
		var row []parquet.Value
		rowMap := item.AsMap()
		for idx, field := range s.schema.Fields() {
			v, err := toParquetValue(field, s.tableSchema[abstract.ColumnName(field.Name())], rowMap[field.Name()], idx)
			if err != nil {
				return xerrors.Errorf("field %v: %w", field.Name(), err)
			}
			row = append(row, *v)
		}
		rows[i] = row
	}

	if _, err := s.writer.WriteRows(rows); err != nil {
		return err
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

func NewParquetStreamSerializer(ostream io.Writer, schema *parquet.Schema, tableSchema abstract.FastTableSchema, compressionCodec compress.Codec) (*parquetStreamSerializer, error) {
	pqSerializer := parquetStreamSerializer{
		schema:           schema,
		writer:           nil,
		tableSchema:      tableSchema,
		compressionCodec: compressionCodec,
	}

	err := pqSerializer.SetStream(ostream)

	if err != nil {
		return nil, err
	}
	return &pqSerializer, nil
}

func NewParquetBatchSerializer(compressionCodec compress.Codec) *parquetBatchSerializer {
	return &parquetBatchSerializer{
		schema:           nil,
		tableSchema:      nil,
		compressionCodec: compressionCodec,
		streamSerializer: nil,
		buffer:           nil,
	}
}
