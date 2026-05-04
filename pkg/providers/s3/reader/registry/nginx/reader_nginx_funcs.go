package reader

import (
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// checkUnexpectedFields returns an error if the line has unconsumed non-whitespace content
// and the behavior is set to Error. Returns nil for Ignore/Unspecified or when there are no extra fields.
func checkUnexpectedFields(line string, consumed int, behavior s3_model.NginxUnexpectedFieldBehavior) error {
	switch behavior {
	case s3_model.NginxUnexpectedFieldBehaviorError:
		if consumed < len(line) && strings.TrimSpace(line[consumed:]) != "" {
			return xerrors.Errorf("unexpected extra fields: %s", strings.TrimSpace(line[consumed:]))
		}
		return nil
	default:
		return nil
	}
}

// convertNginxValue converts a string value to the appropriate Go type based on the column schema.
// Datetime fields (time_local) require explicit parsing; other types are handled by strictify.
// A bare "-" is treated as a missing value and returns nil (ClickHouse will apply its column
// default via insert_null_as_default, same as JSON reader does for null fields).
func convertNginxValue(value string, col abstract.ColSchema) (any, error) {
	if value == "-" {
		return nil, nil
	}
	switch col.DataType {
	case ytschema.TypeDatetime.String(), ytschema.TypeDate.String():

		t, err := time.Parse(timeLocalLayout, value)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse datetime %q for column %s, err: %w", value, col.ColumnName, err)
		}
		return t, nil
	default:
		return value, nil
	}
}

func wrapNginxReadFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("nginx.Read.FlushChunk", err)
}
