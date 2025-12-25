package rps

// Author: kry127
// rps.go -- common file for RPS tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/randutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

// KV is Key-Document object for key-value database (Mongo or in-memory)
type KV struct {
	Key      string      `bson:"_id"`
	Document interface{} `bson:"document"`
}

func stringOrDots(input string) string {
	if len(input) < 64 {
		return input
	} else {
		return input[:61] + "..."
	}
}

func (k *KV) String() string {
	return fmt.Sprintf("KV{%s: %s}", stringOrDots(k.Key), stringOrDots(fmt.Sprintf("%v", k.Document)))
}

// GenerateKV produces random Key-Document object with key and value as
// string of `keysize` and `valsize` length respectfully
func GenerateKV(keysize, valsize int) KV {
	return KV{
		Key:      randutil.GenerateAlphanumericString(keysize),
		Document: bson.D{{Key: "value", Value: randutil.GenerateAlphanumericString(valsize)}},
	}
}

// RpsSpec defines specification of requests count per Delay
// applied in order: delete, create, update
type RpsSpec struct {
	DeleteCount, CreateCount, UpdateCount, ReplaceCount uint
	KVConstructor                                       func() KV `json:"-"`
	Delay                                               time.Duration
}

// RpsNotifier  contains callbacks for different events of RPS generator
type RpsCallbacks struct {
	Tick      func(ctx context.Context, index int, rps *RpsModel) bool
	OnDelete  func(ctx context.Context, key string)
	OnCreate  func(ctx context.Context, entity KV)
	OnUpdate  func(ctx context.Context, previous KV, actual KV)
	OnReplace func(ctx context.Context, previous KV, actual KV)
}

var (
	// default spec immediately gives control to user
	defaultRpsSpec = RpsSpec{
		KVConstructor: func() KV {
			return GenerateKV(10, 10)
		},
		Delay: 0 * time.Millisecond,
	}
	defaultRpsNotifier = RpsCallbacks{
		Tick:      func(ctx context.Context, index int, model *RpsModel) bool { return false },
		OnDelete:  func(ctx context.Context, key string) {},
		OnCreate:  func(ctx context.Context, entity KV) {},
		OnUpdate:  func(ctx context.Context, previous KV, actual KV) {},
		OnReplace: func(ctx context.Context, previous KV, actual KV) {},
	}
)

type historyDescription struct {
	optype          string // insert, update, delete
	opkey           string
	value, oldValue *KV
}

func (h *historyDescription) String() string {
	switch h.optype {
	case "insert":
		return fmt.Sprintf("%s %s", h.optype, h.value.String())
	case "update":
		return fmt.Sprintf("%s %s=>%s", h.optype, h.oldValue.String(), h.value.String())
	case "delete":
		return fmt.Sprintf("%s %s", h.optype, stringOrDots(h.opkey))
	default:
		return fmt.Sprintf("%s %s %s %s", h.optype, stringOrDots(h.opkey), h.value.String(), h.oldValue.String())
	}
}

func HistoryToString(history []historyDescription) string {
	var hd []string
	for _, entry := range history {
		hd = append(hd, entry.String())
	}
	return strings.Join(hd, "\n")
}

// RpsModel is a:
//  1. rate-limiter for requests
//  2. in-mem KV storage
//
// Anyone can access in-memory Persistent state, NonPersistent(deleted) state, and change history as ModelHistory
type RpsModel struct {
	// we will use this for generating RPS: every 'period' milliseconds the 'timer' hits
	timer         *time.Timer
	specification *RpsSpec
	callbacks     *RpsCallbacks
	ctx           context.Context
	ctxCancel     func()
	// we should uniformly distribute documents between this two categories:
	Persistent    map[string]interface{} // present KV
	NonPersistent map[string]struct{}    // sometimes deleted KV
	ModelHistory  map[string][]historyDescription
}

func (r *RpsModel) Close() {
	r.ctxCancel()
	r.timer.Stop()
}

// SetSpec sets RPS specification
func (r *RpsModel) SetSpec(spec *RpsSpec) {
	if spec == nil {
		spec = &defaultRpsSpec
	}
	r.specification = spec

	r.timer.Reset(r.specification.Delay)
}

func (r *RpsModel) CheckValid(t *testing.T, ctx context.Context, label string, coll *mongo.Collection) {
	cursor, err := coll.Find(ctx, bson.D{})
	require.NoError(t, err)
	hasInCursor := map[string]struct{}{}
	for cursor.Next(ctx) {
		var kv KV
		err := cursor.Decode(&kv)
		require.NoError(t, err)
		hasInCursor[kv.Key] = struct{}{}
		history := r.ModelHistory[kv.Key]
		actualVal, persist := r.Persistent[kv.Key]
		_, nonPersist := r.NonPersistent[kv.Key]
		require.True(t, persist, fmt.Sprintf("Entity with label '%s' should persist in model. History: \n%s\n", label, HistoryToString(history)))
		require.False(t, nonPersist, fmt.Sprintf("Entity with label '%s' should not be deleted in model. History: \n%s\n", label, HistoryToString(history)))
		require.Equal(t, actualVal, kv.Document, "Values in label '%s' and model should match. History: \n%s\n", label, HistoryToString(history))
	}
	// extra check on completeness
	for key := range r.Persistent {
		_, ok := hasInCursor[key]
		require.True(t, ok, fmt.Sprintf("All values inserted in model should be presented in database labeled '%s'", label))
	}
	for key := range r.NonPersistent {
		_, notOk := hasInCursor[key]
		require.False(t, notOk, fmt.Sprintf("All values deleted from model should not be presented in database labeled '%s'", label))
	}
}

func (r *RpsModel) Start() {
	tickIndex := 0
	for {
		select {
		case <-r.timer.C:
			shouldContinue := r.callbacks.Tick(r.ctx, tickIndex, r)
			if !shouldContinue {
				return
			}
			tickIndex++

			// make deletes, inserts and updates
			toDelete := r.specification.DeleteCount
			for key, oldValue := range r.Persistent {
				if toDelete == 0 {
					break
				}
				delete(r.Persistent, key)
				r.NonPersistent[key] = struct{}{}
				r.callbacks.OnDelete(r.ctx, key)
				oldKv := KV{Key: key, Document: oldValue}
				r.ModelHistory[key] = append(r.ModelHistory[key], historyDescription{
					optype:   "delete",
					opkey:    key,
					value:    nil,
					oldValue: &oldKv,
				})
				toDelete--
			}

			// make inserts and updates
			toCreate := r.specification.CreateCount
			toReplace := r.specification.ReplaceCount
			toUpdate := r.specification.UpdateCount
			retryLimit, retryLimitID := 20, 0
			for {
				if toCreate == 0 {
					break
				}

				kv := r.specification.KVConstructor()
				if oldValue, ok := r.Persistent[kv.Key]; ok {
					// this is an update
					if toUpdate > 0 {
						r.Persistent[kv.Key] = kv.Document
						oldKv := KV{Key: kv.Key, Document: oldValue}
						r.callbacks.OnUpdate(r.ctx, oldKv, kv)
						r.ModelHistory[kv.Key] = append(r.ModelHistory[kv.Key], historyDescription{
							optype:   "update",
							opkey:    kv.Key,
							value:    &kv,
							oldValue: &oldKv,
						})
						toUpdate--
					} else {
						retryLimitID++
						if retryLimitID == retryLimit {
							// give up on inserting, to many collisions
							logger.Log.Warn("Too many collisions on RPS insert", log.Int("RetryLimit", retryLimit))
							break
						}
					}
				} else {
					if toCreate > 0 {
						// this is an insert
						r.Persistent[kv.Key] = kv.Document
						delete(r.NonPersistent, kv.Key)
						r.callbacks.OnCreate(r.ctx, kv)
						r.ModelHistory[kv.Key] = append(r.ModelHistory[kv.Key], historyDescription{
							optype:   "insert",
							opkey:    kv.Key,
							value:    &kv,
							oldValue: nil,
						})
						toCreate--
					} else if toReplace > 0 {
						// this is an update
						for replaceWhatID, replaceWhatValue := range r.Persistent {
							r.Persistent[kv.Key] = kv.Document
							delete(r.NonPersistent, kv.Key)
							delete(r.Persistent, replaceWhatID)
							r.NonPersistent[replaceWhatID] = struct{}{}
							r.callbacks.OnReplace(r.ctx, KV{Key: replaceWhatID, Document: replaceWhatValue}, kv)
							toReplace--
							break
						}
					}
				}
			}

			// make the rest of updates
			for key, oldValue := range r.Persistent {
				if toUpdate == 0 {
					break
				}
				newValue := r.specification.KVConstructor().Document
				r.Persistent[key] = newValue
				oldKv := KV{Key: key, Document: oldValue}
				newKv := KV{Key: key, Document: newValue}
				r.callbacks.OnUpdate(r.ctx, oldKv, newKv)
				r.ModelHistory[key] = append(r.ModelHistory[key], historyDescription{
					optype:   "update",
					opkey:    key,
					value:    &newKv,
					oldValue: &oldKv,
				})
				toUpdate--
			}

			r.timer.Reset(r.specification.Delay)
		case <-r.ctx.Done():
			return
		}
	}
}

// NewRpsModel creates RPS model
// use initialSpec to set frequency and value of operations
// use onCreate, onDelete and onUpdate from RpsCallbacks to make actions (e.g. with database)
// use Delay in RpsCallbacks to reconfigure RPS in time
func NewRpsModel(ctx context.Context, notifiers *RpsCallbacks) *RpsModel {
	newCtx, newCtxCancel := context.WithCancel(ctx)

	if notifiers == nil {
		notifiers = &defaultRpsNotifier
	}

	r := &RpsModel{
		timer:         time.NewTimer(defaultRpsSpec.Delay),
		specification: &defaultRpsSpec,
		callbacks:     notifiers,
		ctx:           newCtx,
		ctxCancel:     newCtxCancel,
		Persistent:    map[string]interface{}{},
		NonPersistent: map[string]struct{}{},
		ModelHistory:  map[string][]historyDescription{},
	}

	// start periodic goroutine
	return r
}
