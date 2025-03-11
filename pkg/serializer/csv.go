package serializer

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/castx"
	"golang.org/x/xerrors"
)

type csvSerializer struct {
}

type csvStreamSerializer struct {
	serializer csvSerializer
	writer     io.Writer
}

func (s *csvSerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	res := &bytes.Buffer{}
	rowOut := csv.NewWriter(res)
	cells := make([]string, len(item.ColumnValues))
	for i, v := range item.ColumnValues {
		cell, err := castx.ToStringE(v)
		if err != nil {
			rawJSON, err := json.Marshal(v)
			if err != nil {
				return nil, xerrors.Errorf("CsvSerializer: unable to marshal composite cell: %w", err)
			}
			cell = string(rawJSON)
		}
		cells[i] = cell
	}
	if err := rowOut.Write(cells); err != nil {
		return nil, xerrors.Errorf("CsvSerializer: unable to write cells: %w", err)
	}
	rowOut.Flush()
	return res.Bytes(), nil
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
