package reader_error

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	generic_parser "github.com/transferia/transferia/pkg/parsers/generic"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

// HandleDataError applies UnparsedPolicy to a data-layer ReaderError only.
// For ScopeRecord it emits one unparsed change item; for ScopeFile/ScopeSchema it emits a file-level unparsed marker.
func HandleDataError(
	tableID abstract.TableID,
	policy s3_model.UnparsedPolicy,
	readerError ReaderErrorData,
) ([]abstract.ChangeItem, ReaderError) {
	switch policy {
	case s3_model.UnparsedPolicyFail:
		return nil, NewReaderErrorFatal("HandleDataError", readerError)
	case s3_model.UnparsedPolicyRetry:
		return nil, readerError
	default:
		var (
			raw    []byte
			reason string
			idx    int
		)
		reason = readerError.Cause.Error()
		switch readerError.Scope {
		case ScopeRecord:
			raw = fmt.Appendf(nil, "%s:%d", readerError.File, readerError.Row)
			idx = int(readerError.Row)
		case ScopeFile, ScopeSchema:
			raw = fmt.Appendf(nil, "file=%s;scope=%s;op=%s", readerError.File, readerError.Scope, readerError.Op)
			idx = 0
		default:
			raw = fmt.Appendf(nil, "file=%s;scope=%s;op=%s", readerError.File, readerError.Scope, readerError.Op)
			idx = 0
		}
		changeItem := generic_parser.NewUnparsed(
			abstract.NewEmptyPartition(),
			tableID.Name,
			raw,
			reason,
			idx,
			0,
			time.Now(),
		)
		return []abstract.ChangeItem{changeItem}, nil
	}
}
