package yt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestWrapCreateNodeCodecError(t *testing.T) {
	t.Run("non-codec error returned unchanged", func(t *testing.T) {
		origErr := xerrors.New("some other error")
		require.Equal(t, origErr, WrapCreateNodeCodecError(origErr))
	})

	t.Run("invalid compression_codec detected by attribute name", func(t *testing.T) {
		ytErr := yterrors.Err(
			"Error setting builtin attribute \"compression_codec\"",
			yterrors.Err(`Error parsing ECodec value "invalid_codec"`),
		)
		result := WrapCreateNodeCodecError(ytErr)

		var ce coded.CodedError
		require.ErrorAs(t, result, &ce)
		require.Equal(t, codes.YTInvalidTableCompressionCodec, ce.Code())
	})

	t.Run("invalid erasure_codec detected by attribute name", func(t *testing.T) {
		ytErr := yterrors.Err(
			"Error setting builtin attribute \"erasure_codec\"",
			yterrors.Err(`Error parsing ECodec value "bad_erasure"`),
		)
		result := WrapCreateNodeCodecError(ytErr)

		var ce coded.CodedError
		require.ErrorAs(t, result, &ce)
		require.Equal(t, codes.YTInvalidTableErasureCodec, ce.Code())
	})

	t.Run("unrelated YT error not wrapped", func(t *testing.T) {
		ytErr := yterrors.Err("Error setting builtin attribute \"optimize_for\"")
		require.Equal(t, ytErr, WrapCreateNodeCodecError(ytErr))
	})
}

func TestWrapTooManyOperationsError(t *testing.T) {
	ytErr := yterrors.Err(`Limit for the number of concurrent operations 10 for pool "dt" has been reached`, yterrors.CodeTooManyOperations)
	var ce coded.CodedError
	require.ErrorAs(t, WrapTooManyOperationsError(ytErr), &ce)
	require.Equal(t, codes.YTTooManyOperations, ce.Code())

	otherErr := xerrors.New("some other error")
	require.Equal(t, otherErr, WrapTooManyOperationsError(otherErr))
}
