//go:build !disable_clickhouse_provider

package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/transferia/transferia/library/go/core/xerrors"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"go.ytsaurus.tech/library/go/core/log"
)

type httpClientImpl struct {
	config     conn.ConnParams
	httpClient *http.Client
}

func (c *httpClientImpl) buildConnString(host *chconn.Host, database string) string {
	proto := "http"
	if c.config.SSLEnabled() {
		proto = "https"
	}
	return fmt.Sprintf("%s://%s:%d/?database=%s&output_format_write_statistics=0", proto, host.Name, host.HTTPPort, url.QueryEscape(database))
}

func (c *httpClientImpl) prepareQuery(query interface{}) (io.Reader, error) {
	switch q := query.(type) {
	case io.Reader:
		return q, nil
	case string:
		return strings.NewReader(q), nil
	case []byte:
		return bytes.NewReader(q), nil
	default:
		return nil, xerrors.Errorf("expected string, []byte or io.Reader, got %T", q)
	}
}

func (c *httpClientImpl) QueryStream(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}) (io.ReadCloser, error) {
	preparedQuery, err := c.prepareQuery(query)
	if err != nil {
		return nil, xerrors.Errorf("error preparing query: %w", err)
	}

	st := time.Now()
	var compressed bytes.Buffer
	gzw, err := zstd.NewWriter(&compressed, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, xerrors.Errorf("unable to construct writer: %w", err)
	}
	n, err := io.Copy(gzw, preparedQuery)
	if err != nil {
		return nil, xerrors.Errorf("unable to copy buffer for compression: %w", err)
	}
	if err := gzw.Close(); err != nil {
		return nil, xerrors.Errorf("unable to close compressor: %w", err)
	}
	lgr.Infof("compressed: %s / %s in %s", format.SizeInt(compressed.Len()), format.SizeUInt64(uint64(n)), time.Since(st))

	connString := c.buildConnString(host, c.config.Database())
	lgr.Infof("built conn_string: %s", connString)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, connString, bytes.NewReader(compressed.Bytes()))
	if err != nil {
		return nil, coded.Errorf(providers.NetworkUnreachable, "unable to create request: %w", err)
	}

	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-Encoding", "zstd")
	req.Header.Add("X-ClickHouse-User", c.config.User())
	p, err := c.config.ResolvePassword()
	if err != nil {
		return nil, xerrors.Errorf("error resolving password: %w", err)
	}
	if p != "" {
		req.Header.Add("X-ClickHouse-Key", p)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("unable to exec request: %w", err)
	}
	if resp.StatusCode >= 400 {
		rawBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, xerrors.Errorf("failed to read unsuccessful response body: %w", err)
		}
		return nil, xerrors.Errorf("failed: %v to POST %s, status: %s: %w", query, req.URL.String(), resp.Status, ParseCHException(string(rawBody)))
	}
	return resp.Body, nil
}

func (c *httpClientImpl) Query(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}, res interface{}) error {
	body, err := c.QueryStream(ctx, lgr, host, query)
	if err != nil {
		return err
	}
	defer body.Close()
	if res == nil {
		return nil
	}
	if err := json.NewDecoder(body).Decode(res); err != nil {
		return xerrors.Errorf("error decoding ClickHouse response: %w", err)
	}
	return nil
}

func (c *httpClientImpl) Exec(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}) error {
	return c.Query(ctx, lgr, host, query, nil)
}

// error string ex.: "Code: 170. DB::Exception: Requested cluster 'test' not found. (BAD_GET) (version 22.3.12.19 (official build))"
// may contain code name or not. Punctuation may vary among CH versions
var (
	httpExcBaseRe    = regexp.MustCompile(`^.*Code: (?P<code>\d+)\W+(?P<message>.*)`)
	httpExcVersionRe = regexp.MustCompile(`(?P<message>.*)\s+\(version.*`)
	httpExcNameRe    = regexp.MustCompile(`(?P<message>.*)\s+\((?P<name>[A-Z_]+)\)`)
)

func ParseCHException(raw string) error {
	// see https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Exception.cpp#L460 and some code around
	// Try to parse basic Code and Message
	matches := httpExcBaseRe.FindStringSubmatch(raw)
	if len(matches) < 2 {
		return xerrors.Errorf("unable to parse response as clickhouse error, response is `%s`", raw)
	}

	var code int32
	if c, err := strconv.Atoi(matches[1]); err != nil {
		return xerrors.Errorf("error parsing error code from clickhouse error, CH response is `%s`", raw)
	} else {
		code = int32(c)
	}
	msg := matches[2]
	name := ""

	// Try to remove version from message
	matches = httpExcVersionRe.FindStringSubmatch(msg)
	if len(matches) == 2 {
		msg = matches[1]
	}

	// Try to find error name in message
	matches = httpExcNameRe.FindStringSubmatch(msg)
	if len(matches) == 3 {
		msg = matches[1]
		name = matches[2]
	}

	return &clickhouse.Exception{
		Code:       code,
		Name:       name,
		Message:    msg,
		StackTrace: "",
	}
}

func NewHTTPClientImpl(config conn.ConnParams) (*httpClientImpl, error) {
	tlsConf, err := conn.NewTLS(config)
	if err != nil {
		return nil, xerrors.Errorf("error getting TLS config")
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConf,
		},
		Timeout: time.Hour * 100,
	}
	return &httpClientImpl{
		config:     config,
		httpClient: client,
	}, nil
}
