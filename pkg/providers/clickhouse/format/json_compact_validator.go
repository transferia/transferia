//go:build !disable_clickhouse_provider

package format

import (
	"encoding/json"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type JSONCompactValidator struct {
	expectedColumnsCount int
	decoder              *json.Decoder
}

func (e *JSONCompactValidator) ReadAndValidate() (int64, error) {
	startOffset := e.decoder.InputOffset()
	var tmp []json.RawMessage
	if err := e.decoder.Decode(&tmp); err != nil {
		return e.decoder.InputOffset() - startOffset,
			xerrors.Errorf("string is invalid json, err: %w", err)
	}
	if e.expectedColumnsCount != len(tmp) {
		return e.decoder.InputOffset() - startOffset,
			xerrors.Errorf("json string contains wrong number of columns, expected: %d, real: %d", e.expectedColumnsCount, len(tmp))
	}
	return e.decoder.InputOffset() - startOffset, nil
}

func NewJSONCompactValidator(reader io.Reader, columnsCount int) *JSONCompactValidator {
	return &JSONCompactValidator{
		expectedColumnsCount: columnsCount,
		decoder:              json.NewDecoder(reader),
	}
}
