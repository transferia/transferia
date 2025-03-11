package serializer

import (
	"bytes"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

type parquetStreamSerializer struct {
	schema      *parquet.Schema
	writer      *parquet.GenericWriter[struct{}]
	tableSchema abstract.FastTableSchema
}

// works via stream serializer
type parquetBatchSerializer struct {
	schema      *parquet.Schema
	tableSchema abstract.FastTableSchema
}

func (s *parquetBatchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	var buffer = bytes.NewBuffer(make([]byte, 0))
	if s.schema == nil {
		parquetSchema, err := BuildParquetSchema(items[0].TableSchema.FastColumns())
		if err != nil {
			return nil, xerrors.Errorf("s3_sink: failed to create serializer: %w", err)
		}
		s.schema = parquetSchema
		s.tableSchema = items[0].TableSchema.FastColumns()
	}
	streamSerializer, err := NewParquetStreamSerializer(buffer, s.schema, s.tableSchema)
	if err != nil {
		return nil, xerrors.Errorf("ParquetBatchSerialize: unable to build underlying stream serializer: %w", err)
	}
	if err := streamSerializer.Serialize(items); err != nil {
		return nil, xerrors.Errorf("ParquetBatchSerialize: unable to serialize items: %w", err)
	}
	if err := streamSerializer.Close(); err != nil {
		return nil, xerrors.Errorf("ParquetBatchSerialize: unable to serialize items: %w", err)
	}
	return buffer.Bytes(), nil
}

func (s *parquetStreamSerializer) SetStream(ostream io.Writer) error {
	if err := s.Close(); err != nil {
		return xerrors.Errorf("parquetStreamSerializer: failed to close sink: %w", err)
	}

	s.writer = parquet.NewGenericWriter[struct{}](ostream, s.schema)

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

func NewParquetStreamSerializer(ostream io.Writer, schema *parquet.Schema, tableSchema abstract.FastTableSchema) (*parquetStreamSerializer, error) {
	pqSerializer := parquetStreamSerializer{
		schema:      schema,
		writer:      nil,
		tableSchema: tableSchema,
	}

	err := pqSerializer.SetStream(ostream)

	if err != nil {
		return nil, err
	}
	return &pqSerializer, nil
}

func NewParquetBatchSerializer() *parquetBatchSerializer {
	return &parquetBatchSerializer{
		schema:      nil,
		tableSchema: nil,
	}
}
