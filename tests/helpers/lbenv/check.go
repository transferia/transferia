package lbenv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

func MessageDataForCanon(t *testing.T, msg persqueue.ReadMessage) map[string]any {
	var asMap map[string]any
	require.NoError(t, jsonx.Unmarshal(msg.Data, &asMap))
	delete(asMap, "ts")
	delete(asMap, "caller")
	return asMap
}

func initAndStartReader(t *testing.T, lbEnv *recipe.Env, database, topic string) (persqueue.Reader, context.CancelFunc) {
	readerOptions := lbEnv.ConsumerOptions()
	readerOptions.Database = database
	readerOptions.Topics = []persqueue.TopicInfo{{Topic: topic}}
	readerOptions.WithProxy(fmt.Sprintf("%s:%d", lbEnv.Endpoint, lbEnv.Port))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	reader := persqueue.NewReaderV1(readerOptions)
	_, err := reader.Start(ctx)
	require.NoError(t, err)
	return reader, cancel
}

type MessageHandler func(topic string, index int, msg persqueue.ReadMessage)

var stubMessageHandler MessageHandler = func(_ string, _ int, _ persqueue.ReadMessage) {}

func LoadMessages(t *testing.T, lbEnv *recipe.Env, database, topic string, expectedNum int, handler MessageHandler) {
	if handler == nil {
		handler = stubMessageHandler
	}
	reader, cancelFunc := initAndStartReader(t, lbEnv, database, topic)
	defer cancelFunc()

	index := 0
	for e := range reader.C() {
		switch event := e.(type) {
		case *persqueue.Data:
			for _, batch := range event.Batches() {
				for _, msg := range batch.Messages {
					handler(batch.Topic, index, msg)
					index++
				}
			}
			event.Commit()
			if index >= expectedNum {
				reader.Shutdown()
			}

		case *persqueue.CommitAck:
		case *persqueue.LockV1:
			event.StartRead(true, event.ReadOffset, event.ReadOffset)
		default:
			t.Fatalf("Received unexpected event %T", event)
		}
	}

	err := reader.Err()
	if err == context.DeadlineExceeded {
		err = nil
	}
	require.Equal(t, expectedNum, index)
	require.NoError(t, err)
}

func CheckData(t *testing.T, lbEnv *recipe.Env, database, topic string, checkFunc func(msg string) bool, maxDuration time.Duration) {
	sleepTime := 100 * time.Millisecond
	startTime := time.Now()

	reader, cancelFunc := initAndStartReader(t, lbEnv, database, topic)
	defer cancelFunc()

	checked := false
	for e := range reader.C() {
		require.Less(t, time.Since(startTime), maxDuration, "Exceeded max allowed duration checks")
		switch event := e.(type) {
		case *persqueue.Data:
			for _, batch := range event.Batches() {
				for _, msg := range batch.Messages {
					checked = checked || checkFunc(string(msg.Data))
				}
			}
			event.Commit()
			if checked {
				reader.Shutdown()
			}
		case *persqueue.CommitAck:
		case *persqueue.LockV1:
			event.StartRead(true, event.ReadOffset, event.ReadOffset)
		default:
			t.Fatalf("Received unexpected event %T", event)
		}
		logger.Log.Warnf("Check func returned false, will sleep %v and retry", sleepTime)
		time.Sleep(sleepTime)
	}

	err := reader.Err()
	if err == context.DeadlineExceeded {
		err = nil
	}
	require.NoError(t, err)
}
