package csv

import "github.com/transferia/transferia/library/go/core/xerrors"

var (
	errInvalidDelimiter     = xerrors.NewSentinel("csv: invalid delimiter")
	errDoubleQuotesDisabled = xerrors.NewSentinel("csv: found double quotes while double quote feature disabled")
	errQuotingDisabled      = xerrors.NewSentinel("csv: found quote char while feature disabled")
)
