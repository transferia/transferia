package line

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	parsers_scanner "github.com/transferia/transferia/pkg/parsers/scanner"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

func readLines(content []byte) ([]string, uint64, error) {
	currScanner := parsers_scanner.NewLineBreakScanner(content)
	scannedLines, err := currScanner.ScanAll()
	if err != nil {
		return nil, 0, xerrors.Errorf("failed to scan lines, err: %w", err)
	}
	bytesRead := 0

	for _, scannedLine := range scannedLines {
		bytesRead += len(scannedLine) + 1 // 1 - it's 'len("\n")'
	}

	return scannedLines, uint64(bytesRead), nil
}

func wrapLineReadFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("line.Read.FlushChunk", err)
}
