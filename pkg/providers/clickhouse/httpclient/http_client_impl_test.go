//go:build !disable_clickhouse_provider

package httpclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
)

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
		_, err = cl.QueryStream(context.Background(), logger.Log, connHost, bytes.NewReader(expectedData))
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
