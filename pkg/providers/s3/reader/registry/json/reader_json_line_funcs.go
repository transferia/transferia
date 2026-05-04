package json

import (
	"bufio"
	"io"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func guessType(value any) (ytschema.Type, string, error) {
	switch result := value.(type) {
	case map[string]any:
		return ytschema.TypeAny, "object", nil
	case []any:
		return ytschema.TypeAny, "array", nil
	case string:
		if _, err := cast.ToTimeE(result); err == nil {
			return ytschema.TypeTimestamp, "timestamp", nil
		}
		return ytschema.TypeString, "string", nil
	case bool:
		return ytschema.TypeBoolean, "boolean", nil
	case float64:
		return ytschema.TypeFloat64, "number", nil
	default:
		return ytschema.TypeAny, "", xerrors.New("unknown json type")
	}
}

func readSingleJSONObject(reader *bufio.Reader, fileKey string) (string, error) {
	content, err := io.ReadAll(reader)
	if err != nil && !xerrors.Is(err, io.EOF) {
		return "", xerrors.Errorf("failed to read file %s, err: %w", fileKey, err)
	}

	extractedLine := make([]rune, 0)
	foundStart := false
	countCurlyBrackets := 0
	for _, char := range string(content) {
		if foundStart && countCurlyBrackets == 0 {
			break
		}

		extractedLine = append(extractedLine, char)
		if char == '{' {
			countCurlyBrackets++
			foundStart = true
			continue
		}

		if char == '}' {
			countCurlyBrackets--
		}
	}
	return string(extractedLine), nil
}

func wrapJSONLReadFlushChunk(suffix string, err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("jsonl.Read.FlushChunk."+suffix, err)
}
