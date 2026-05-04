package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// TestProtoTailUnparsedRetryAsDataFile documents base.md: stream tail retry uses NewErrDataFile (not Internal).
func TestProtoTailUnparsedRetryAsDataFile(t *testing.T) {
	errorData, ok := reader_error.AsReaderErrorData(reader_error.NewReaderErrorDataFile("proto.streamParseFile.tailUnparsedRetry", "f", xerrors.New("tail unparsed (retry)")))
	require.True(t, ok)
	_, err := reader_error.HandleDataError(abstract.TableID{Namespace: "n", Name: "t"}, s3_model.UnparsedPolicyRetry, errorData)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}
