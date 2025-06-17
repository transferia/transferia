//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestKeyBatcher(t *testing.T) {
	t.Run("BatchSizeFlushing", triggerBatchSizeFlushing)
	t.Run("KeySizeFlushing", triggerKeySizeFlushing)
	t.Run("IntervalFlushing", triggerIntervalFlushing)
}

func triggerBatchSizeFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     128,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}

	triggered := false
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered = true
		return []sizedFullDocument{}, nil
	}
	documentPusher := func(ctx context.Context, event *changeEvent) error {
		return nil
	}
	batcher, err := NewKeyBatcher(context.TODO(), logger.Log, fullDocumentExtractor, documentPusher, &params)
	require.NoError(t, err)
	defer func(batcher *keyBatcher) {
		if err := batcher.Close(); err != nil {
			logger.Log.Error("Batcher error", log.Error(err))
		}
	}(batcher)

	value := 0
	for x := uint(0); x < params.BatchSizeLimit; x++ {
		require.False(t, triggered, "Batcher should not be triggered yet")
		err := batcher.PushKeyChangeEvent(&keyChangeEvent{
			keyEvent: &KeyChangeEvent{
				OperationType: "insert",
				DocumentKey:   DocumentKey{ID: value},
				Namespace:     Namespace{},
				ToNamespace:   Namespace{},
				ClusterTime:   ToMongoTimestamp(time.Now()),
			},
			size:       0,
			decodeTime: 0,
		})
		require.NoError(t, err)
	}

	require.True(t, triggered, "Batcher should have been triggered already (by amount of documents heuristics)")
}

func triggerKeySizeFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1024,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}

	triggered := false
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered = true
		return []sizedFullDocument{}, nil
	}
	documentPusher := func(ctx context.Context, event *changeEvent) error {
		return nil
	}
	batcher, err := NewKeyBatcher(context.TODO(), logger.Log, fullDocumentExtractor, documentPusher, &params)
	require.NoError(t, err)
	defer func(batcher *keyBatcher) {
		if err := batcher.Close(); err != nil {
			logger.Log.Error("Batcher error", log.Error(err))
		}
	}(batcher)

	value := 0
	for x := uint64(0); x < params.KeySizeThreshold; x++ {
		require.False(t, triggered, "Batcher should not be triggered yet")
		err := batcher.PushKeyChangeEvent(&keyChangeEvent{
			keyEvent: &KeyChangeEvent{
				OperationType: "insert",
				DocumentKey:   DocumentKey{ID: value},
				Namespace:     Namespace{},
				ToNamespace:   Namespace{},
				ClusterTime:   ToMongoTimestamp(time.Now()),
			},
			size:       1,
			decodeTime: 0,
		})
		require.NoError(t, err)
	}

	require.True(t, triggered, "Batcher should have been triggered already (by key size heuristic)")
}

func triggerIntervalFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 3 * time.Second,
	}

	triggered := false
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered = true
		return []sizedFullDocument{}, nil
	}
	documentPusher := func(ctx context.Context, event *changeEvent) error {
		return nil
	}
	batcherCreationTime := time.Now()
	batcher, err := NewKeyBatcher(context.TODO(), logger.Log, fullDocumentExtractor, documentPusher, &params)
	require.NoError(t, err)
	defer func(batcher *keyBatcher) {
		if err := batcher.Close(); err != nil {
			logger.Log.Error("Batcher error", log.Error(err))
		}
	}(batcher)

	ctx, cancel := context.WithCancel(context.TODO())
	go func(ctx context.Context) {
		value := 0
		for x := uint(0); x < params.BatchSizeLimit; x++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if time.Since(batcherCreationTime) < params.BatchFlushInterval {
				require.False(t, triggered, "Batcher should not be triggered yet")
			}
			err := batcher.PushKeyChangeEvent(&keyChangeEvent{
				keyEvent: &KeyChangeEvent{
					OperationType: "insert",
					DocumentKey:   DocumentKey{ID: value},
					Namespace:     Namespace{},
					ToNamespace:   Namespace{},
					ClusterTime:   ToMongoTimestamp(time.Now()),
				},
				size:       1,
				decodeTime: 0,
			})
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		}
	}(ctx)

	time.Sleep(params.BatchFlushInterval + time.Second)
	cancel()
	require.True(t, triggered, "Batcher should have been triggered already (by ticker)")
}
