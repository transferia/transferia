package snapshot

import (
	"context"
	"io"
	"time"

	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
)

// Main keeps the DDL admission sessions alive while secondary workers load data.
// Keepalive only checks existing sessions: it can never reopen a closed gate.
type stagingDDLSessionKeepalive struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newStagingDDLSessionKeepalive(logger log.Logger, client httpclient.HTTPClient, host *conn_clickhouse.Host, closeClient func(), sessionID string, probeInterval time.Duration) *stagingDDLSessionKeepalive {
	ctx, cancel := context.WithCancel(context.Background())
	keepalive := &stagingDDLSessionKeepalive{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		defer close(keepalive.done)
		defer closeClient()

		ticker := time.NewTicker(probeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			probeCtx, stopProbe := context.WithTimeout(ctx, 10*time.Second)
			responseBody, err := client.QueryStream(probeCtx, logger, host, "SELECT 1 FORMAT Null", ch_eo_common.ExistingSessionParams(sessionID))
			if err == nil {
				_, err = io.Copy(io.Discard, responseBody)
				_ = responseBody.Close()
			}
			stopProbe()
			if err != nil && ctx.Err() == nil {
				// A busy session is already alive. An expired session stays closed;
				// the next staging command fails safely. Never recreate it here.
				logger.Debug("Snapshot staging session keepalive failed", log.String("host", host.Name), log.Error(err))
			}
		}
	}()
	return keepalive
}

func (s *stagingDDLSessionKeepalive) stopAndWait() {
	s.cancel()
	<-s.done
}
