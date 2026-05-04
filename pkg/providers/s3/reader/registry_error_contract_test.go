package reader

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// asReaderErrorTransport returns (ReaderErrorTransport, true) if err unwraps to that type.
func asReaderErrorTransport(err error) (reader_error.ReaderErrorTransport, bool) {
	var none reader_error.ReaderErrorTransport
	for err != nil {
		switch e := err.(type) {
		case reader_error.ReaderErrorTransport:
			return e, true
		case *reader_error.ReaderErrorTransport:
			if e != nil {
				return *e, true
			}
		}
		err = xerrors.Unwrap(err)
	}

	return none, false
}

func TestAsReaderErrorOnWrappedTransport(t *testing.T) {
	err := reader_error.NewReaderErrorTransport("op", "file.csv", xerrors.New("root"))
	tr, ok := asReaderErrorTransport(err)
	require.True(t, ok)
	require.Equal(t, "op", tr.Op())
	require.Equal(t, "file.csv", tr.File())
}

func TestNewReaderErrorDataRecordHandleDataErrorPolicies(t *testing.T) {
	table := abstract.TableID{Namespace: "ns", Name: "tbl"}
	wrapped := reader_error.NewReaderErrorDataRecord("op", "f.csv", 7, xerrors.New("bad value"))
	errorData, ok := reader_error.AsReaderErrorData(wrapped)
	require.True(t, ok)
	require.NotNil(t, errorData)

	items, err := reader_error.HandleDataError(table, s3_model.UnparsedPolicyContinue, errorData)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, parsers.IsUnparsed(items[0]))

	_, err = reader_error.HandleDataError(table, s3_model.UnparsedPolicyFail, errorData)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	_, err = reader_error.HandleDataError(table, s3_model.UnparsedPolicyRetry, errorData)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}

func TestDataCausefInsideNewErrDataSchema(t *testing.T) {
	err := reader_error.NewReaderErrorDataSchema("op", "k", xerrors.New("leaf"))
	errorData, ok := reader_error.AsReaderErrorData(err)
	require.True(t, ok)
	require.Equal(t, reader_error.ScopeSchema, errorData.Scope)
}
