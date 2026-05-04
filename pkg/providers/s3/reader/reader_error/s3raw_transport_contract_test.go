package reader_error

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
)

// TestS3rawErrorsStayUnclassifiedAtS3rawBoundary documents base.md: s3raw returns printf-shaped
// transport errors (s3raw.TransportErrFmt); they are not *ReaderError until a format reader wraps
// them with NewErrTransport (covered by registry transport matrix tests per format).
func TestS3rawErrorsStayUnclassifiedAtS3rawBoundary(t *testing.T) {
	err := xerrors.Errorf(s3raw.TransportErrFmt, "op", xerrors.New("cause"))
	_, ok := AsReaderErrorData(err)
	require.False(t, ok, "s3raw layer returns raw errors; readers must classify with NewErrTransport")
}
