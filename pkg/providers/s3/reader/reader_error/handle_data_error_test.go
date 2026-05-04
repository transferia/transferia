package reader_error

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

func TestHandleDataErrorRecordContinue(t *testing.T) {
	table := abstract.TableID{Namespace: "ns", Name: "tbl"}
	errorData, ok := AsReaderErrorData(NewReaderErrorDataRecord("op", "f.csv", 3, xerrors.New("bad row")))
	require.True(t, ok)
	items, err := HandleDataError(table, s3_model.UnparsedPolicyContinue, errorData)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, parsers.IsUnparsed(items[0]))
}

func TestHandleDataErrorRecordFail(t *testing.T) {
	table := abstract.TableID{Namespace: "ns", Name: "tbl"}
	errorData, ok := AsReaderErrorData(NewReaderErrorDataRecord("op", "f.csv", 1, xerrors.New("bad")))
	require.True(t, ok)
	_, err := HandleDataError(table, s3_model.UnparsedPolicyFail, errorData)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
}

func TestHandleDataErrorRecordRetry(t *testing.T) {
	table := abstract.TableID{Namespace: "ns", Name: "tbl"}
	errorData, ok := AsReaderErrorData(NewReaderErrorDataRecord("op", "f.csv", 2, xerrors.New("bad")))
	require.True(t, ok)
	_, err := HandleDataError(table, s3_model.UnparsedPolicyRetry, errorData)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}

func TestHandleDataErrorFileScopeContinue(t *testing.T) {
	table := abstract.TableID{Namespace: "ns", Name: "tbl"}
	errorData, ok := AsReaderErrorData(NewReaderErrorDataFile("csv.schema", "f.csv", xerrors.New("no header")))
	require.True(t, ok)
	items, err := HandleDataError(table, s3_model.UnparsedPolicyContinue, errorData)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, parsers.IsUnparsed(items[0]))
}
