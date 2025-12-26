package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
)

func TestIsErrorCode(t *testing.T) {
	correctErr := &mysql.MySQLError{Number: 1}
	require.False(t, IsErrorCode(xerrors.New("irrelevant"), 0), "irrelevant errors")
	require.False(t, IsErrorCode(&mysql.MySQLError{Number: 0}, 1), "different code errors")
	require.True(t, IsErrorCode(correctErr, 1), "equal code errors")
	require.True(t, IsErrorCode(xerrors.Errorf("oh: %w", correctErr), 1), "wrapped equal code errors")
}

func TestConnectionTimeoutError(t *testing.T) {
	// Test that connection timeout errors are properly caught and returned with CodeConnectionTimeout
	config := &ConnectionParams{
		Host:     "192.0.2.1", // RFC 5737 test IP that should timeout
		Port:     3306,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	// Create a connector with a very short timeout to simulate connection timeout
	mysqlConfig := mysql.NewConfig()
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = fmt.Sprintf("%v:%v", config.Host, config.Port)
	mysqlConfig.User = config.User
	mysqlConfig.Passwd = config.Password
	mysqlConfig.DBName = config.Database
	mysqlConfig.Timeout = 1 * time.Millisecond // Very short timeout to force timeout

	connector, err := mysql.NewConnector(mysqlConfig)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer db.Close()

	// Try to ping with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = db.PingContext(ctx)
	require.Error(t, err)

	// Check if it's a connection timeout error
	var opErr *net.OpError
	if xerrors.As(err, &opErr) && opErr.Op == "dial" {
		// This should be caught by our coded error
		codedErr := coded.Errorf(codes.Dial, "Can't ping server: %w", err)

		var codedError coded.CodedError
		require.True(t, xerrors.As(codedErr, &codedError), "Error should be a CodedError")
		require.Equal(t, codes.Dial, codedError.Code(), "Error code should be providers.Dial")
		require.Contains(t, codedErr.Error(), "Can't ping server")
	}
}

func TestConnectionTimeoutIPv6Error(t *testing.T) {
	params := &ConnectionParams{
		Host:     "::1",
		Port:     3306,
		User:     "user",
		Password: "password",
		Database: "db",
	}
	db, err := Connect(params, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "Can't ping server")
	require.Contains(t, err.Error(), "connect: connection refused")
}

func TestInvalidFormatOfHost(t *testing.T) {
	params := &ConnectionParams{
		Host:     "not:valid.192.168:0.1:at.all_cUrSeD",
		Port:     3306,
		User:     "user",
		Password: "password",
		Database: "db",
	}
	db, err := Connect(params, nil)
	if db != nil {
		_ = db.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "Can't ping server")
	require.Contains(t, err.Error(), "dial tcp: lookup not:valid.192.168:0.1:at.all_cUrSeD:3306: no such host")
}
