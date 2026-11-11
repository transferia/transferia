package snapshot

import (
	"context"
	"io"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"go.uber.org/mock/gomock"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestStagingKeepaliveStopCancelsProbeAndReleasesClient(test *testing.T) {
	controller := gomock.NewController(test)
	client := httpclient.NewMockHTTPClient(controller)
	host := &conn_clickhouse.Host{Name: "localhost", HTTPPort: 8123}
	started := make(chan struct{})
	var released atomic.Int32
	client.EXPECT().QueryStream(gomock.Any(), gomock.Any(), host, "SELECT 1 FORMAT Null", gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ log.Logger, _ *conn_clickhouse.Host, _ interface{}, params url.Values) (io.ReadCloser, error) {
			if params.Get("session_id") != "existing" || params.Get("session_check") != "1" {
				test.Errorf("probe must not recreate the session: %v", params)
			}
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	).Times(1)
	keepalive := newStagingDDLSessionKeepalive(logger.Log, client, host, func() { released.Add(1) }, "existing", time.Millisecond)
	test.Cleanup(keepalive.stopAndWait)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		test.Fatal("keepalive probe did not start")
	}
	keepalive.stopAndWait()
	require.EqualValues(test, 1, released.Load())
	keepalive.stopAndWait()
	require.EqualValues(test, 1, released.Load())
}
