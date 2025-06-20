//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type keyChangeEvent struct {
	keyEvent   *KeyChangeEvent
	size       int
	decodeTime time.Duration
}

func (k *keyChangeEvent) toChangeEvent() *changeEvent {
	if k == nil {
		return nil
	}
	return &changeEvent{
		event:      k.keyEvent.ToChangeEvent(),
		size:       k.size,
		decodeTime: k.decodeTime,
	}
}

type changeEvent struct {
	event      *ChangeEvent
	size       int
	decodeTime time.Duration
}

type puChangeEvent struct {
	changeEvent
	parallelizationUnit ParallelizationUnit
}

type changeEventPusher func(ctx context.Context, event *changeEvent) error

// ChangeStreamWatcher produces changeEvents
// encapsulates method with which full documents of mongo collections are retrieved
type ChangeStreamWatcher interface {
	// Watch is one-shot method. After return all allocated resources should be freed with calling Close
	Watch(context.Context, changeEventPusher) error
	Close(context.Context)
	// GetResumeToken returns last resume token to watch from, or nil if info is not available
	// this may be used in future for chaining watchers in the oplog during fallback
	GetResumeToken() bson.Raw
}
