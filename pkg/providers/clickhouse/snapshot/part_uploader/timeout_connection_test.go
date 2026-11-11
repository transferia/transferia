package part_uploader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

func TestSnapshotNativeConnectionBoundsBlockedIO(test *testing.T) {
	for _, operation := range []string{"read", "write"} {
		test.Run(operation, func(test *testing.T) {
			client, server := net.Pipe()
			defer client.Close()

			defer server.Close()

			connection := newTimeoutConnection(client, 20*time.Millisecond)
			var err error
			if operation == "read" {
				_, err = connection.Read(make([]byte, 1))
			} else {
				_, err = connection.Write([]byte{1})
			}
			var networkError net.Error
			require.ErrorAs(test, err, &networkError)
			require.True(test, networkError.Timeout())
		})
	}
}

func TestSnapshotNativeConnectionPreservesCustomDialer(test *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	defer server.Close()

	called := false
	options := clickhouse_go.Options{
		DialContext: func(ctx context.Context, address string) (net.Conn, error) {
			called = true
			require.Equal(test, "snapshot-host:9000", address)
			require.NoError(test, ctx.Err())
			return client, nil
		},
	}
	configureIOTimeouts(&options)
	connection, err := options.DialContext(context.Background(), "snapshot-host:9000")
	require.NoError(test, err)
	require.True(test, called)
	wrapped, ok := connection.(*timeoutConnection)
	require.True(test, ok)
	require.Same(test, client, wrapped.Conn)
	require.Equal(test, defaultIOTimeout, wrapped.ioTimeout)
}

func TestSnapshotNativeConnectionPreservesTLS(test *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()

	rootCertificates := x509.NewCertPool()
	rootCertificates.AddCert(server.Certificate())
	options := clickhouse_go.Options{TLS: &tls.Config{RootCAs: rootCertificates}}
	configureIOTimeouts(&options)
	connection, err := options.DialContext(context.Background(), server.Listener.Addr().String())
	require.NoError(test, err)
	defer connection.Close()

	wrapped := connection.(*timeoutConnection)
	tlsConnection, ok := wrapped.Conn.(*tls.Conn)
	require.True(test, ok)
	require.True(test, tlsConnection.ConnectionState().HandshakeComplete)
	require.NotEmpty(test, tlsConnection.ConnectionState().VerifiedChains)
}
