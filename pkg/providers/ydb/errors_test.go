package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

func requireCode(t *testing.T, err error, expected coded.Code) {
	var ce coded.CodedError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, expected, ce.Code())
}

func TestWrapYDBError(t *testing.T) {
	t.Run("nil, unknown and already coded errors are returned unchanged", func(t *testing.T) {
		require.NoError(t, WrapYDBError(nil))
		unknown := xerrors.New("unable to cast Decimal to string")
		require.Equal(t, unknown, WrapYDBError(unknown))
		alreadyCoded := xerrors.Errorf("driver: %w", coded.Errorf(error_codes.YDBNotFound, "cluster discovery failed: %w", unknown))
		require.Equal(t, alreadyCoded, WrapYDBError(alreadyCoded))
	})

	t.Run("transport unavailable", func(t *testing.T) {
		err := xerrors.Errorf("stream read table error: %w", grpc_status.Error(grpc_codes.Unavailable, "connection reset by peer"))
		requireCode(t, WrapYDBError(err), error_codes.YDBUnavailable)
	})

	t.Run("text-only cases", func(t *testing.T) {
		cases := map[string]coded.Code{
			"unable to describe path, path:notification_new, err:no access (address:...)":                                error_codes.YDBAccessDenied,
			"Cannot create YDB driver: cluster discovery failed (endpoint:\"ydb-ru.yandex.net:2135\")":                   error_codes.YDBConnectionFailed,
			"Cannot create YDB driver: failed to dial \"ydbproxy.ydb.cloud.yandex.net:2135\": context deadline exceeded": error_codes.YDBConnectionFailed,
			"Cannot create YDB driver: data source name '<invalid DSN>' wrong":                                           error_codes.YDBConnectionFailed,
		}
		for text, expected := range cases {
			requireCode(t, WrapYDBError(xerrors.New(text)), expected)
		}
	})
}
