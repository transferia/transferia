package yt

import (
	"context"
	"regexp"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	reCompressionCodecError = regexp.MustCompile(`Error setting builtin attribute "compression_codec"`)
	reErasureCodecError     = regexp.MustCompile(`Error setting builtin attribute "erasure_codec"`)
)

// WrapYTError marks well-known YT failures with error codes; already coded and unknown errors are returned unchanged.
func WrapYTError(err error) error {
	var alreadyCoded coded.CodedError
	if code := ytErrorCode(err); code != "" && !xerrors.As(err, &alreadyCoded) {
		return coded.New(code, err)
	}
	return err
}

func ytErrorCode(err error) coded.Code {
	switch {
	case err == nil:
		return ""
	case yterrors.ContainsMessageRE(err, reCompressionCodecError):
		return codes.YTInvalidTableCompressionCodec
	case yterrors.ContainsMessageRE(err, reErasureCodecError):
		return codes.YTInvalidTableErasureCodec
	case yterrors.ContainsErrorCode(err, yterrors.CodeAccountLimitExceeded):
		return codes.YTAccountLimitExceeded
	case yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError):
		return codes.YTAccessDenied
	case yterrors.ContainsErrorCode(err, yterrors.CodeResolveError):
		return codes.YTPathNotFound
	case yterrors.ContainsErrorCode(err, yterrors.CodeTooManyOperations):
		return codes.YTTooManyOperations
	case isLockConflict(err):
		return codes.YTLockConflict
	case yterrors.ContainsErrorCode(err, yterrors.CodeUnavailable), yterrors.ContainsErrorCode(err, yterrors.CodeTimeout),
		xerrors.Is(err, context.DeadlineExceeded), strings.Contains(err.Error(), context.DeadlineExceeded.Error()),
		strings.Contains(err.Error(), "could not find any available backend"):
		return codes.YTUnavailable
	}
	return ""
}

func isLockConflict(err error) bool {
	return yterrors.ContainsErrorCode(err, yterrors.CodeSameTransactionLockConflict) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeDescendantTransactionLockConflict) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict) ||
		yterrors.ContainsErrorCode(err, yterrors.CodePendingLockConflict) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeTransactionLockConflict)
}
