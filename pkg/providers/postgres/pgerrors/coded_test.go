package pgerrors

import (
	"testing"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

func pgErr(code, message string) error {
	return xerrors.Errorf("failed to execute SELECT: %w", &pgconn.PgError{Code: code, Message: message, Severity: "ERROR"})
}

func TestWrap(t *testing.T) {
	t.Run("nil, unknown and already coded errors are returned unchanged", func(t *testing.T) {
		require.NoError(t, Wrap(nil))
		unknown := pgErr("42P01", "relation does not exist")
		require.Equal(t, unknown, Wrap(unknown))
		alreadyCoded := xerrors.Errorf("ddl: %w", coded.Errorf(error_codes.PostgresDDLLockTimeout, "lock: %w", pgErr("55P03", "canceling statement due to lock timeout")))
		require.Equal(t, alreadyCoded, Wrap(alreadyCoded))
	})

	t.Run("sqlstates are mapped", func(t *testing.T) {
		cases := map[error]coded.Code{
			pgErr("53300", "too many active clients for user (pool_size for user x reached 10)"): error_codes.PostgresTooManyConnections,
			pgErr("55P03", "canceling statement due to lock timeout"):                            error_codes.PostgresLockTimeout,
			pgErr("3D000", "database \"x\" does not exist"):                                      error_codes.PostgresDatabaseNotFound,
			pgErr("28P01", "password authentication failed for user \"x\""):                      error_codes.InvalidCredential,
		}
		for err, expected := range cases {
			var ce coded.CodedError
			require.ErrorAs(t, Wrap(err), &ce)
			require.Equal(t, expected, ce.Code(), err.Error())
		}
	})
}
