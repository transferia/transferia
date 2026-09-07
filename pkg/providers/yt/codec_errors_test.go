package yt

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func requireCode(t *testing.T, err error, expected coded.Code) {
	var ce coded.CodedError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, expected, ce.Code())
}

func TestWrapYTError(t *testing.T) {
	t.Run("nil and unknown errors returned unchanged", func(t *testing.T) {
		require.NoError(t, WrapYTError(nil))
		origErr := xerrors.New("some other error")
		require.Equal(t, origErr, WrapYTError(origErr))
		ytErr := yterrors.Err("Error setting builtin attribute \"optimize_for\"")
		require.Equal(t, ytErr, WrapYTError(ytErr))
	})

	t.Run("already coded error is not overridden", func(t *testing.T) {
		inner := coded.Errorf(codes.YTValueSizeLimitExceeded, "row too large: %w", yterrors.Err("timeout", yterrors.CodeTimeout))
		wrapped := xerrors.Errorf("write failed: %w", inner)
		require.Equal(t, wrapped, WrapYTError(wrapped))
	})

	t.Run("invalid codecs detected by attribute name", func(t *testing.T) {
		requireCode(t, WrapYTError(yterrors.Err(
			"Error setting builtin attribute \"compression_codec\"",
			yterrors.Err(`Error parsing ECodec value "invalid_codec"`),
		)), codes.YTInvalidTableCompressionCodec)
		requireCode(t, WrapYTError(yterrors.Err(
			"Error setting builtin attribute \"erasure_codec\"",
			yterrors.Err(`Error parsing ECodec value "bad_erasure"`),
		)), codes.YTInvalidTableErasureCodec)
	})

	t.Run("yt error codes are mapped", func(t *testing.T) {
		cases := map[yterrors.ErrorCode]coded.Code{
			yterrors.CodeAccountLimitExceeded:              codes.YTAccountLimitExceeded,
			yterrors.CodeAuthorizationError:                codes.YTAccessDenied,
			yterrors.CodeResolveError:                      codes.YTPathNotFound,
			yterrors.CodeTooManyOperations:                 codes.YTTooManyOperations,
			yterrors.CodeConcurrentTransactionLockConflict: codes.YTLockConflict,
			yterrors.CodeUnavailable:                       codes.YTUnavailable,
			yterrors.CodeTimeout:                           codes.YTUnavailable,
		}
		for ytCode, expected := range cases {
			requireCode(t, WrapYTError(yterrors.Err("Error creating node", yterrors.Err("inner", ytCode))), expected)
		}
	})

	t.Run("client-side failures are mapped by message", func(t *testing.T) {
		requireCode(t, WrapYTError(xerrors.Errorf("call start_transaction failed: %w", context.DeadlineExceeded)), codes.YTUnavailable)
		requireCode(t, WrapYTError(xerrors.New("load balancer could not find any available backend (code: 1000000)")), codes.YTUnavailable)
	})
}
