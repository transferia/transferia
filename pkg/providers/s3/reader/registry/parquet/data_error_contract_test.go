package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// TestParquetDataFileUsesPolicyGateway documents base.md: parquet file/row data errors are routed through
// NewErrDataFile / NewReaderErrorDataRecord + HandleDataError (see reader_parquet.go). This is the same contract
// as other readers; a full S3+parquet integration test would require a tiny binary golden file.
func TestParquetDataFileUsesPolicyGateway(t *testing.T) {
	t.Helper()
	errorData, ok := reader_error.AsReaderErrorData(reader_error.NewReaderErrorDataFile("parquet.ReadRow", "f.pq", xerrors.New("read row")))
	require.True(t, ok)
	items, err := reader_error.HandleDataError(abstract.TableID{Namespace: "n", Name: "t"}, s3_model.UnparsedPolicyContinue, errorData)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, parsers.IsUnparsed(items[0]))
}
