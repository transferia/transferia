package format

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/slices"
	"go.ytsaurus.tech/library/go/core/log"
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
		msg := fmt.Sprintf("JSON string contains wrong number of columns, expected: %d, real: %d", e.expectedColumnsCount, len(tmp))
		logger.Log.Warn(msg, log.Strings("rows", slices.Map(tmp, func(x json.RawMessage) string { return string(x) })))
		return e.decoder.InputOffset() - startOffset, xerrors.New(msg)
	}
	return e.decoder.InputOffset() - startOffset, nil
}

func NewJSONCompactValidator(reader io.Reader, columnsCount int) *JSONCompactValidator {
	return &JSONCompactValidator{
		expectedColumnsCount: columnsCount,
		decoder:              json.NewDecoder(reader),
	}
}
