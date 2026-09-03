package yt

import (
	"regexp"

	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	reCompressionCodecError = regexp.MustCompile(`Error setting builtin attribute "compression_codec"`)
	reErasureCodecError     = regexp.MustCompile(`Error setting builtin attribute "erasure_codec"`)
)

// WrapCreateNodeCodecError inspects CreateNode error. If it is invalid codec message then wraps it as coded error.
// If the error is not codec-related, it is returned unchanged.
func WrapCreateNodeCodecError(err error) error {
	if yterrors.ContainsMessageRE(err, reCompressionCodecError) {
		return coded.Errorf(codes.YTInvalidTableCompressionCodec,
			"invalid table_compression_codec: %w", err)
	}
	if yterrors.ContainsMessageRE(err, reErasureCodecError) {
		return coded.Errorf(codes.YTInvalidTableErasureCodec,
			"invalid table_erasure_codec: %w", err)
	}
	return err
}

// WrapTooManyOperationsError marks the pool concurrent operations limit error with a code.
func WrapTooManyOperationsError(err error) error {
	if yterrors.ContainsErrorCode(err, yterrors.CodeTooManyOperations) {
		return coded.Errorf(codes.YTTooManyOperations, "pool operations limit reached: %w", err)
	}
	return err
}
