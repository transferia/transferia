package httpclient

import (
	"context"
	"io"

	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"go.ytsaurus.tech/library/go/core/log"
)

// how to generate mock from 'client' and 'writer' interfaces:
// > ya tool mockgen -source ./http_client.go -package httpclient -destination ./http_client_mock.go

type HTTPClient interface {
	Query(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}, res interface{}) error
	QueryStream(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}) (io.ReadCloser, error)
	Exec(ctx context.Context, lgr log.Logger, host *chconn.Host, query interface{}) error
}
