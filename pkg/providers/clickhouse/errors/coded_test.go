package errors

import (
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

func TestWrapConnectError(t *testing.T) {
	require.NoError(t, WrapConnectError(nil))

	unknown := xerrors.Errorf("query error: %w", &clickhouse.Exception{Code: 81, Message: "Database db does not exist"})
	require.Equal(t, unknown, WrapConnectError(unknown))

	dial := xerrors.Errorf("query error: %w", &net.OpError{Op: "dial", Net: "tcp", Err: xerrors.New("i/o timeout")})
	require.True(t, error_codes.Dial.Contains(WrapConnectError(dial)))
	dns := xerrors.Errorf("query error: %w", &net.DNSError{Err: "no such host", Name: "ch.example"})
	require.True(t, error_codes.Dial.Contains(WrapConnectError(dns)))

	auth := xerrors.Errorf("query error: %w", &clickhouse.Exception{Code: authenticationFailedCode, Message: "Authentication failed"})
	require.True(t, error_codes.InvalidCredential.Contains(WrapConnectError(auth)))

	alreadyCoded := coded.Errorf(error_codes.ClickHouseSSLRequired, "ssl: %w", auth)
	require.Equal(t, alreadyCoded, WrapConnectError(alreadyCoded))
}
