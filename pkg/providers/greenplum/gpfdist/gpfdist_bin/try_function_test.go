//go:build !disable_greenplum_provider

package gpfdistbin

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestErrorInterface(t *testing.T) {
	err := newCancelFailedError(xerrors.New("error"))
	require.True(t, xerrors.As(err, new(CancelFailedError)))

	var cancelErr1 CancelFailedError
	require.True(t, xerrors.As(err, &cancelErr1))
	require.Equal(t, err, cancelErr1)

	wrappedErr := xerrors.Errorf("unable to fail: %w", err)
	require.True(t, xerrors.As(wrappedErr, new(CancelFailedError)))

	var cancelErr2 CancelFailedError
	require.True(t, xerrors.As(wrappedErr, &cancelErr2))
	require.Equal(t, err, cancelErr2)
}
