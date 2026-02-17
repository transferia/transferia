package serializer

import (
	"bytes"
	"encoding/csv"
	"io"

	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/xerrors"
)

type csvSerializer struct {
}

type csvStreamSerializer struct {
	serializer csvSerializer
	writer     io.Writer
}

// buildCsvCells converts all ChangeItem column values to CSV string cells.
// Uses toCsvValue for type-aware conversion when TableSchema is available.
func buildCsvCells(item *abstract.ChangeItem) ([]string, error) {
	cells := make([]string, len(item.ColumnValues))
	var columns []abstract.ColSchema
	if item.TableSchema != nil {
		columns = item.TableSchema.Columns()
	}
	for i, v := range item.ColumnValues {
		var col *abstract.ColSchema
		if i < len(columns) {
			col = &columns[i]
		}
		cell, err := toCsvValue(v, col)
		if err != nil {
			return nil, xerrors.Errorf("column %d: %w", i, err)
		}
		cells[i] = cell
	}
	return cells, nil
}

func (s *csvSerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	res := &bytes.Buffer{}
	rowOut := csv.NewWriter(res)
	cells, err := buildCsvCells(item)
	if err != nil {
		return nil, xerrors.Errorf("CsvSerializer: %w", err)
	}
	if err := rowOut.Write(cells); err != nil {
		return nil, xerrors.Errorf("CsvSerializer: unable to write cells: %w", err)
	}
	rowOut.Flush()
	return res.Bytes(), nil
}

func (s *csvSerializer) SerializeWithSeparatorTo(item *abstract.ChangeItem, separator []byte, buf *bytes.Buffer) error {
	rowOut := csv.NewWriter(buf)
	cells, err := buildCsvCells(item)
	if err != nil {
		return xerrors.Errorf("CsvSerializer: %w", err)
	}
	if err := rowOut.Write(cells); err != nil {
		return xerrors.Errorf("CsvSerializer: unable to write cells: %w", err)
	}
	rowOut.Flush()

	if len(separator) > 0 {
		if _, err := buf.Write(separator); err != nil {
			return xerrors.Errorf("CsvSerializer: unable to write separator: %w", err)
		}
	}

	return nil
}

func (s *csvSerializer) Close() ([]byte, error) {
	return nil, nil
}

func (s *csvStreamSerializer) Serialize(items []*abstract.ChangeItem) error {
	for _, item := range items {
		data, err := s.serializer.Serialize(item)
		if err != nil {
			return xerrors.Errorf("CsvStreamSerializer: failed to serialize item: %w", err)
		}
		_, err = s.writer.Write(data)
		if err != nil {
			return xerrors.Errorf("CsvStreamSerializer: failed to write data: %w", err)
		}
	}
	return nil
}

func (s *csvStreamSerializer) Close() error {
	return nil
}

func NewCsvSerializer() *csvSerializer {
	return &csvSerializer{}
}

func NewCsvStreamSerializer(ostream io.Writer) *csvStreamSerializer {
	return &csvStreamSerializer{
		serializer: *NewCsvSerializer(),
		writer:     ostream,
	}
}
