//go:build !disable_clickhouse_provider

package format

import (
	"encoding/csv"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type CsvValidator struct {
	expectedColumnsCount int
	reader               *csv.Reader
}

func (e *CsvValidator) ReadAndValidate() (int64, error) {
	startOffset := e.reader.InputOffset()
	tmp, err := e.reader.Read()
	if err != nil {
		return e.reader.InputOffset() - startOffset,
			xerrors.Errorf("string is invalid csv, err: %w", err)
	}
	if e.expectedColumnsCount != len(tmp) {
		return e.reader.InputOffset() - startOffset,
			xerrors.Errorf("json string contains wrong number of columns, expected: %d, real: %d", e.expectedColumnsCount, len(tmp))
	}
	return e.reader.InputOffset() - startOffset, nil
}

func NewCsvValidator(reader io.Reader, columnsCount int) *CsvValidator {
	csvReader := csv.NewReader(reader)
	csvReader.TrimLeadingSpace = true
	return &CsvValidator{
		expectedColumnsCount: columnsCount,
		reader:               csvReader,
	}
}
