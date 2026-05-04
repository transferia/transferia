package json

import (
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

func wrapJSONParserReadFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("json.parser.Read.FlushChunk", err)
}
