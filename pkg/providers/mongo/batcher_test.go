package mongo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	t.Run("MultiNamespaceFetch", multiNamespaceFetch)
	t.Run("FailedNamespaceFetch", failedNamespaceFetch)
}

// multiNamespaceFetch checks that a flush fetches full documents of different namespaces concurrently
// and still delivers a correct event per key, with the DDL event after all of them.
func multiNamespaceFetch(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}

	var concurrent, maxConcurrent int64
	var concurrentMu sync.Mutex
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		concurrentMu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		concurrentMu.Unlock()
		defer func() {
			concurrentMu.Lock()
			concurrent--
			concurrentMu.Unlock()
		}()
		time.Sleep(100 * time.Millisecond) // parks the goroutine so other fetches get scheduled
		result := make([]sizedFullDocument, 0, len(inList))
		for _, key := range inList {
			result = append(result, sizedFullDocument{
				document:   bson.D{{Key: "_id", Value: key}, {Key: "ns", Value: ns.GetFullName()}},
				decodeTime: 0,
				size:       1,
			})
		}
		return result, nil
	}

	var pushedMu sync.Mutex
	var pushed []*changeEvent
	documentPusher := func(ctx context.Context, event *changeEvent) error {
		pushedMu.Lock()
		pushed = append(pushed, event)
		pushedMu.Unlock()
		return nil
	}

	batcher, err := NewKeyBatcher(context.TODO(), logger.Log, fullDocumentExtractor, documentPusher, &params)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, batcher.Close())
	}()

	const namespaces = 4
	const keysPerNamespace = 10
	for nsIdx := 0; nsIdx < namespaces; nsIdx++ {
		ns := Namespace{Database: "db", Collection: fmt.Sprintf("coll_%d", nsIdx)}
		for key := 0; key < keysPerNamespace; key++ {
			require.NoError(t, batcher.PushKeyChangeEvent(&keyChangeEvent{
				keyEvent: &KeyChangeEvent{
					OperationType: "insert",
					DocumentKey:   DocumentKey{ID: key},
					Namespace:     ns,
					ToNamespace:   Namespace{},
					ClusterTime:   ToMongoTimestamp(time.Now()),
				},
				size:       1,
				decodeTime: 0,
			}))
		}
	}
	// "drop" goes through the default branch: flushes everything accumulated, then pushes itself
	require.NoError(t, batcher.PushKeyChangeEvent(&keyChangeEvent{
		keyEvent: &KeyChangeEvent{
			OperationType: "drop",
			DocumentKey:   DocumentKey{ID: nil},
			Namespace:     Namespace{Database: "db", Collection: "coll_0"},
			ToNamespace:   Namespace{},
			ClusterTime:   ToMongoTimestamp(time.Now()),
		},
		size:       1,
		decodeTime: 0,
	}))

	pushedMu.Lock()
	defer pushedMu.Unlock()
	require.Len(t, pushed, namespaces*keysPerNamespace+1)
	perNamespace := map[string]int{}
	for _, event := range pushed[:namespaces*keysPerNamespace] {
		require.NotNil(t, event.event.FullDocument)
		fullDocument := event.event.FullDocument.Map()
		require.Equal(t, event.event.DocumentKey.ID, fullDocument["_id"])
		perNamespace[event.event.Namespace.GetFullName()]++
	}
	for ns, count := range perNamespace {
		require.Equal(t, keysPerNamespace, count, "namespace %s", ns)
	}
	require.Equal(t, "drop", pushed[len(pushed)-1].event.OperationType)
	require.GreaterOrEqual(t, maxConcurrent, int64(2), "fetches of different namespaces should overlap")
}

// failedNamespaceFetch checks that an error of one namespace fetch fails the flush and nothing of the batch is pushed.
func failedNamespaceFetch(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		if ns.Collection == "broken" {
			return nil, fmt.Errorf("fetch of %s failed", ns.GetFullName())
		}
		time.Sleep(50 * time.Millisecond)
		result := make([]sizedFullDocument, 0, len(inList))
		for _, key := range inList {
			result = append(result, sizedFullDocument{document: bson.D{{Key: "_id", Value: key}}, decodeTime: 0, size: 1})
		}
		return result, nil
	}
	var pushed atomic.Int64
	documentPusher := func(ctx context.Context, event *changeEvent) error {
		pushed.Add(1)
		return nil
	}

	batcher, err := NewKeyBatcher(context.TODO(), logger.Log, fullDocumentExtractor, documentPusher, &params)
	require.NoError(t, err)
	defer func() { _ = batcher.Close() }()

	for _, coll := range []string{"ok", "broken"} {
		require.NoError(t, batcher.PushKeyChangeEvent(&keyChangeEvent{
			keyEvent: &KeyChangeEvent{
				OperationType: "insert",
				DocumentKey:   DocumentKey{ID: 1},
				Namespace:     Namespace{Database: "db", Collection: coll},
				ToNamespace:   Namespace{},
				ClusterTime:   ToMongoTimestamp(time.Now()),
			},
			size:       1,
			decodeTime: 0,
		}))
	}
	err = batcher.PushKeyChangeEvent(&keyChangeEvent{ // flushes synchronously
		keyEvent: &KeyChangeEvent{
			OperationType: "drop",
			DocumentKey:   DocumentKey{ID: nil},
			Namespace:     Namespace{Database: "db", Collection: "ok"},
			ToNamespace:   Namespace{},
			ClusterTime:   ToMongoTimestamp(time.Now()),
		},
		size:       1,
		decodeTime: 0,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "db.broken failed")
	require.Equal(t, int64(0), pushed.Load(), "a batch with a failed fetch must not be pushed partially")
}

func triggerBatchSizeFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     128,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}

	var triggered atomic.Bool // the extractor runs in a fetch goroutine
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered.Store(true)
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
		require.False(t, triggered.Load(), "Batcher should not be triggered yet")
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

	require.Eventually(t, triggered.Load, 5*time.Second, 10*time.Millisecond,
		"Batcher should have been triggered already (by amount of documents heuristics)")
}

func triggerKeySizeFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1024,
		BatchFlushInterval: 24 * 365.25 * time.Hour,
	}

	var triggered atomic.Bool // the extractor runs in a fetch goroutine
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered.Store(true)
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
		require.False(t, triggered.Load(), "Batcher should not be triggered yet")
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

	require.Eventually(t, triggered.Load, 5*time.Second, 10*time.Millisecond,
		"Batcher should have been triggered already (by key size heuristic)")
}

func triggerIntervalFlushing(t *testing.T) {
	params := BatcherParameters{
		BatchSizeLimit:     1000000000,
		KeySizeThreshold:   1000000000,
		BatchFlushInterval: 3 * time.Second,
	}

	var triggered atomic.Bool // the extractor runs in a fetch goroutine
	fullDocumentExtractor := func(ctx context.Context, ns Namespace, inList bson.A) ([]sizedFullDocument, error) {
		triggered.Store(true)
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
				require.False(t, triggered.Load(), "Batcher should not be triggered yet")
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

	require.Eventually(t, triggered.Load, params.BatchFlushInterval+2*time.Second, 10*time.Millisecond,
		"Batcher should have been triggered already (by ticker)")
	cancel()
}
