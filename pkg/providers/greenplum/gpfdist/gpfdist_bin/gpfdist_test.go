package gpfdistbin

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestTryFunction(t *testing.T) {
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

func TestLocationBrackets(t *testing.T) {
	g := &Gpfdist{localAddr: net.ParseIP("192.168.1.5"), port: 8500, pipeName: "data"}
	require.Equal(t, "gpfdist://192.168.1.5:8500/data", g.Location())

	g = &Gpfdist{localAddr: net.ParseIP("fe80::1234"), port: 8501, pipeName: "data"}
	require.Equal(t, "gpfdist://[fe80::1234]:8501/data", g.Location())
}
