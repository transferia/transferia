package snapshot

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestSinkCloseReleasesOwnedHTTPConnection(test *testing.T) {
	for _, failNativeClose := range []bool{false, true} {
		name := "success"
		if failNativeClose {
			name = "native_close_error"
		}
		test.Run(name, func(test *testing.T) {
			idleConnections := make(chan net.Conn, 1)
			closedConnections := make(chan net.Conn, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				writer.WriteHeader(http.StatusNoContent)
			}))
			// The connection must be released by Sink.Close, not a server idle timer.
			server.Config.IdleTimeout = 0
			server.Config.ConnState = func(connection net.Conn, state http.ConnState) {
				switch state {
				case http.StateIdle:
					idleConnections <- connection
				case http.StateClosed:
					closedConnections <- connection
				}
			}
			server.Start()
			defer server.Close()
			address := server.Listener.Addr().(*net.TCPAddr)
			cfg := new(clickhouse_model.ChDestination)
			cfg.Database = "test"
			cfg.Cleanup = model.Drop
			params, err := cfg.ToSinkParams(new(model.Transfer))
			require.NoError(test, err)
			client, err := httpclient.NewHTTPClientImpl(params)
			require.NoError(test, err)
			defer client.CloseIdleConnections()
			host := new(conn_clickhouse.Host)
			host.Name = address.IP.String()
			host.HTTPPort = address.Port
			database, databaseMock, err := sqlmock.New()
			require.NoError(test, err)
			nativeCloseError := stderrors.New("native close failed")
			closeExpectation := databaseMock.ExpectClose()
			if failNativeClose {
				closeExpectation.WillReturnError(nativeCloseError)
			}
			sink, err := newTestSink(
				logger.Log,
				cfg,
				newSerialState(),
				"transfer",
				"operation",
				[][]ch_eo_common.Node{{ch_eo_common.NewNode(host, database, client)}},
				false,
				sharding.ConstSharder(),
				nil,
				nil,
				nil,
			)
			require.NoError(test, err)
			// newTestSink borrows its node clients. Give this sink the same
			// transport ownership that NewSinkFromTransfer establishes.
			httpCloseCalls := 0
			sink.closeHTTPClient = func() {
				httpCloseCalls++
				client.CloseIdleConnections()
			}
			defer sink.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, host.Name, "SELECT 1 FORMAT Null", nil))
			var idleConnection net.Conn
			select {
			case idleConnection = <-idleConnections:
			case <-ctx.Done():
				test.Fatal("HTTP connection did not become idle")
			}
			select {
			case <-closedConnections:
				test.Fatal("HTTP connection closed before the sink")
			default:
			}

			err = sink.Close()
			if failNativeClose {
				require.ErrorIs(test, err, nativeCloseError)
			} else {
				require.NoError(test, err)
			}
			select {
			case closedConnection := <-closedConnections:
				require.Same(test, idleConnection, closedConnection)
			case <-ctx.Done():
				test.Fatal("Sink.Close left its HTTP connection open")
			}
			require.NoError(test, sink.Close())
			require.Equal(test, 1, httpCloseCalls)
			require.NoError(test, databaseMock.ExpectationsWereMet())
		})
	}
}
