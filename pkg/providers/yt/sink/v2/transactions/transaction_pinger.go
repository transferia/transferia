//go:build !disable_yt_provider

package transactions

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

const pingPeriod = 3 * time.Second

func beginTransactionPinger(client yt.Client, txID yt.TxID, logger log.Logger) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go pingLoop(ctx, client, txID, logger)
	return cancel
}

func pingLoop(ctx context.Context, client yt.Client, txID yt.TxID, logger log.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := client.PingTx(ctx, txID, nil); err != nil {
			logger.Warn("unable to ping main transaction", log.Any("tx_id", txID), log.Error(err))
		}
		time.Sleep(pingPeriod)
	}
}
