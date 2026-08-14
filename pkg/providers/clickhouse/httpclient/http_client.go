package httpclient

import (
	"context"
	"io"
	"net/url"

	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"go.ytsaurus.tech/library/go/core/log"
)

// how to generate mock from 'client' and 'writer' interfaces:
// > ya tool mockgen -source ./http_client.go -package httpclient -destination ./http_client_mock.go

type HTTPClient interface {
	Query(ctx context.Context, lgr log.Logger, host *conn_clickhouse.Host, query interface{}, res interface{}, queryParams url.Values) error
	QueryStream(ctx context.Context, lgr log.Logger, host *conn_clickhouse.Host, query interface{}, queryParams url.Values) (io.ReadCloser, error)
	Exec(ctx context.Context, lgr log.Logger, host *conn_clickhouse.Host, query interface{}, queryParams url.Values) error
}
