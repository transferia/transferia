package parquet

import (
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// openParquetFromReaderClassifiesParquetPanics returns DataFile (not a bare panic) when parquet-go
// cannot open a corrupt/invalid file (see base.md Parquet corrupt-file contract).
func openParquetFromReaderClassifiesParquetPanics(filePath string, r io.ReaderAt) (pr *parquet.Reader, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			pr = nil
			// parquet-go panics with the error of the underlying reader: keep it in the chain, so that
			// a decryption failure is not mistaken for a corrupt file.
			if recErr, ok := rec.(error); ok {
				err = xerrors.Errorf("Parquet panic on file: %s: %w", filePath, recErr)
				return
			}
			err = xerrors.Errorf("Parquet panic on file: %s, recover(): %v", filePath, rec)
		}
	}()

	pr = parquet.NewReader(r)

	_ = pr.Schema()
	_ = pr.NumRows()
	return pr, nil
}

func wrapParquetReadFlushChunk(step string, err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError(step, err)
}
