package httpclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
)

func TestQueryStreamPreservesHTTPStatusAndClickHouseException(test *testing.T) {
	for _, statusCode := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusForbidden} {
		test.Run(strconv.Itoa(statusCode), func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(statusCode)
				if statusCode == http.StatusForbidden {
					_, _ = io.WriteString(writer, "Code: 516. DB::Exception: Authentication failed (AUTHENTICATION_FAILED)")
					return
				}
				_, _ = io.WriteString(writer, "proxy is unavailable")
			}))
			defer server.Close()
			address := server.Listener.Addr().(*net.TCPAddr)
			client, err := NewHTTPClientImpl(&stubParams{port: address.Port})
			require.NoError(test, err)
			defer client.httpClient.CloseIdleConnections()
			body, err := client.QueryStream(context.Background(), logger.Log, &chconn.Host{Name: "localhost", HTTPPort: address.Port}, "SELECT 1 FORMAT Null", nil)
			require.Nil(test, body)
			var responseError *ResponseError
			require.ErrorAs(test, err, &responseError)
			require.Equal(test, statusCode, responseError.StatusCode)
			if statusCode == http.StatusForbidden {
				var serverError *clickhouse_go.Exception
				require.ErrorAs(test, err, &serverError)
				require.EqualValues(test, 516, serverError.Code)
			}
		})
	}
}

var _ conn.ConnParams = (*stubParams)(nil)

type stubParams struct {
	port int
}

func (s stubParams) RootCertPaths() []string {
	return nil
}

func (s stubParams) User() string {
	return "user"
}

func (s stubParams) Password() string {
	return "password"
}

func (s stubParams) ResolvePassword() (string, error) {
	return s.Password(), nil
}

func (s stubParams) Database() string {
	return "default"
}

func (s stubParams) SSLEnabled() bool {
	return false
}

func (s stubParams) PemFileContent() string {
	return ""
}

func (s stubParams) ReadTimeout() time.Duration {
	return 5 * time.Minute
}

const jsonLine = `{"glossary":{"title":"example glossary","GlossDiv":{"title":"S","GlossList":{"GlossEntry":{"ID":"SGML","SortAs":"SGML","GlossTerm":"Standard Generalized Markup Language","Acronym":"SGML","Abbrev":"ISO 8879:1986","GlossDef":{"para":"A meta-markup language, used to create markup languages such as DocBook.","GlossSeeAlso":["GML","XML"]},"GlossSee":"markup"}}}}}
`

func TestCompression(t *testing.T) {
	tc := func(expectedData []byte) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			encoding := r.Header.Get("Content-Encoding")
			require.Equal(t, encoding, "zstd")
			rawData, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			rr, err := zstd.NewReader(bytes.NewReader(rawData))
			require.NoError(t, err)
			unzippedData, err := io.ReadAll(rr)
			require.NoError(t, err)
			_, _ = w.Write([]byte("Hello World"))
			require.Less(t, len(rawData), len(unzippedData))
			require.Equal(t, string(unzippedData), string(expectedData))
		}))

		tcpAddr, ok := testServer.Listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		cl, err := NewHTTPClientImpl(&stubParams{port: tcpAddr.Port})
		require.NoError(t, err)
		connHost := &chconn.Host{
			Name:       "localhost",
			HTTPPort:   tcpAddr.Port,
			NativePort: tcpAddr.Port,
		}
		_, err = cl.QueryStream(context.Background(), logger.Log, connHost, bytes.NewReader(expectedData), nil)
		require.NoError(t, err)
	}
	t.Run("one-json", func(t *testing.T) {
		tc([]byte(jsonLine))
	})
	t.Run("couple-jsons", func(t *testing.T) {
		tc([]byte(strings.Repeat(jsonLine, 8)))
	})
	t.Run("a-lot-of-jsons", func(t *testing.T) {
		tc([]byte(strings.Repeat(jsonLine, 200_000)))
	})
	t.Run("MOAR-jsons", func(t *testing.T) {
		tc([]byte(strings.Repeat(jsonLine, 2_000_000)))
	})
}

func TestParseCHExceptionTooManyPartitions(t *testing.T) {
	raw := "Code: 252. DB::Exception: Too many partitions for single INSERT block (more than 100). " +
		"The limit is controlled by 'max_partitions_per_insert_block' setting. " +
		"Large number of partitions is a common misconception. (TOO_MANY_PARTS) (version 23.8.14.6 (official build))"
	err := ParseCHException(raw)
	exception := new(clickhouse_go.Exception)
	require.True(t, xerrors.As(err, &exception))
	require.Equal(t, int32(252), exception.Code)
	require.Equal(t, "TOO_MANY_PARTS", exception.Name)
	require.Contains(t, exception.Message, "Too many partitions for single INSERT block")
}

func TestQueryStream_ReturnsNetworkUnreachableOnInvalidURL(t *testing.T) {
	cl, err := NewHTTPClientImpl(&stubParams{port: 0})
	require.NoError(t, err)

	badHost := &chconn.Host{
		Name:       "bad host name",
		HTTPPort:   -1,
		NativePort: -1,
	}

	_, err = cl.QueryStream(context.Background(), logger.Log, badHost, "SELECT 1", nil)
	require.Error(t, err)
	if !codes.NetworkUnreachable.Contains(err) {
		t.Fatalf("expected codes.NetworkUnreachable, got: %v", err)
	}
}
