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
