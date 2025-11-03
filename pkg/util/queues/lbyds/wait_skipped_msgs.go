package lbyds

import (
	"context"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"go.ytsaurus.tech/library/go/core/log"
)

func WaitSkippedMsgs(logger log.Logger, consumer persqueue.Reader, inType string) {
	logger.Infof("Start gracefully close %s reader", inType)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	for {
		select {
		case m := <-consumer.C():
			switch v := m.(type) {
			case *persqueue.Data:
				logger.Info(
					"skipped messages",
					log.Any("cookie", v.Cookie),
					log.Any("offsets", BuildMapPartitionToLbOffsetsRange(ConvertBatches(v.Batches()))),
				)
			case *persqueue.Disconnect:
				if v.Err != nil {
					logger.Infof("Disconnected: %v", v.Err.Error())
				} else {
					logger.Info("Disconnected")
				}
			case nil:
				logger.Info("Semi-gracefully closed")
				return
			default:
				logger.Infof("Received unexpected Event type: %T", m)
			}
		case <-consumer.Closed():
			logger.Info("Gracefully closed")
			return
		case <-shutdownCtx.Done():
			logger.Warn("Timeout while waiting for graceful reader shutdown", log.Any("reader_stat", consumer.Stat()))
			return
		}
	}
}
