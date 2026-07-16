package pgerrors

import (
	"testing"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestIsPgError(t *testing.T) {
	correctErr := &pgconn.PgError{Code: string(ErrcWrongObjectType)}

	t.Run("nil error", func(t *testing.T) {
		require.False(t, IsPgError(nil, ErrcWrongObjectType))
	})

	t.Run("irrelevant error", func(t *testing.T) {
		require.False(t, IsPgError(xerrors.New("irrelevant"), ErrcWrongObjectType))
	})

	t.Run("different code", func(t *testing.T) {
		require.False(t, IsPgError(&pgconn.PgError{Code: "a"}, ErrcWrongObjectType))
	})

	t.Run("matching code", func(t *testing.T) {
		require.True(t, IsPgError(correctErr, ErrcWrongObjectType))
	})

	t.Run("wrapped once", func(t *testing.T) {
		require.True(t, IsPgError(xerrors.Errorf("oh: %w", correctErr), ErrcWrongObjectType))
	})

	t.Run("wrapped twice", func(t *testing.T) {
		wrapped := xerrors.Errorf("outer: %w", xerrors.Errorf("inner: %w", correctErr))
		require.True(t, IsPgError(wrapped, ErrcWrongObjectType))
	})
}

func TestAnyErrHasCode(t *testing.T) {
	authErr := &pgconn.PgError{Code: string(ErrcInvalidPassword)}
	connErr := &pgconn.PgError{Code: string(ErrcTooManyConnections)}
	plainErr := xerrors.New("plain error")
	wrappedAuthErr := xerrors.Errorf("connect failed: %w", authErr)

	t.Run("single matching error", func(t *testing.T) {
		require.True(t, AnyErrHasCode([]error{authErr}, ErrcInvalidPassword))
	})

	t.Run("match not in first position", func(t *testing.T) {
		require.True(t, AnyErrHasCode([]error{plainErr, connErr, authErr}, ErrcInvalidPassword))
	})

	t.Run("wrapped error matches", func(t *testing.T) {
		require.True(t, AnyErrHasCode([]error{wrappedAuthErr}, ErrcInvalidPassword))
	})

	t.Run("no match", func(t *testing.T) {
		require.False(t, AnyErrHasCode([]error{plainErr, connErr}, ErrcInvalidPassword))
	})

	t.Run("multiple codes to match", func(t *testing.T) {
		require.True(t, AnyErrHasCode([]error{connErr}, ErrcInvalidPassword, ErrcTooManyConnections))
	})

	t.Run("empty slice", func(t *testing.T) {
		require.False(t, AnyErrHasCode([]error{}, ErrcInvalidPassword))
	})

	t.Run("nil error in slice", func(t *testing.T) {
		require.NotPanics(t, func() {
			require.False(t, AnyErrHasCode([]error{nil, plainErr}, ErrcInvalidPassword))
		})
	})

	t.Run("no codes to match", func(t *testing.T) {
		require.False(t, AnyErrHasCode([]error{authErr}))
	})
}
