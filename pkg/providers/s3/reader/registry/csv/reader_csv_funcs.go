package csv

import (
	dt_csv "github.com/transferia/transferia/pkg/csv"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// skipRows reads and skips the specified amount of rows.
//
//nolint:descriptiveerrors
func skipRows(nrOfRowsToSkip int64, csvReader *dt_csv.Reader, fileKey string) reader_error.ReaderError {
	for range nrOfRowsToSkip {

		_, err := csvReader.ReadLine()
		if err != nil {
			return reader_error.NewReaderErrorDataSchema("csv.skipRows", fileKey, err)
		}
	}
	return nil
}

// readAfterNRows reads and skips the specified amount of csv rows.
// As csv row here a full and complete row is intended (multiline rows are considered as 1 row if so configured).
// It returns the first row read after skipping the specified rows.
//
//nolint:descriptiveerrors
func readAfterNRows(nrOfRowsToSkip int64, csvReader *dt_csv.Reader, fileKey string) ([]string, reader_error.ReaderError) {
	if err := skipRows(nrOfRowsToSkip, csvReader, fileKey); err != nil {
		return nil, reader_error.WrapReaderError("csv.readAfterNRows.skipRows", err)
	}

	elements, err := csvReader.ReadLine()
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("csv.readAfterNRows", fileKey, err)
	}
	return elements, nil
}

func wrapCSVReadFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("csv.Read.FlushChunk", err)
}
