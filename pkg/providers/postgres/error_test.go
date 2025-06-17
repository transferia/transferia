//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestIsPgError(t *testing.T) {
	correctErr := &pgconn.PgError{Code: string(ErrcWrongObjectType)}
	require.False(t, IsPgError(xerrors.New("irrelevant"), ErrcWrongObjectType), "irrelevant error")
	require.False(t, IsPgError(&pgconn.PgError{Code: "a"}, ErrcWrongObjectType), "different code errors")
	require.True(t, IsPgError(correctErr, ErrcWrongObjectType), "equal code errors")
	require.True(t, IsPgError(xerrors.Errorf("oh: %w", correctErr), ErrcWrongObjectType), "wrapped equal code errors")
}
