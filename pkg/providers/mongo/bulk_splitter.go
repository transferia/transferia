package mongo

import (
	mongo_driver "go.mongodb.org/mongo-driver/mongo"
)

// bulkSplitter groups the writes of one write shard into bulks holding at most one write per document, so that a
// bulk can be applied unordered. A write to a document already in the current bulk cuts the bulk and starts a new
// one, or, with fold on, is folded into the earlier write when it supersedes it (see supersedingWrite). The bulks of
// a shard are applied one after another, which keeps the per-document order.
//
// Folding matters on hot documents (counters, statuses, feeds): without it every repeat of a document in a push
// cuts the shard, and a push of hundreds of documents crumbles into bulks of a few documents each, a round trip
// and a storage transaction apiece. Fold is off for OrderedWrites: there the position of a write in the bulk is its
// application order across documents, which folding would change. It is also off for a collection with a unique
// secondary index: a folded bulk may apply the last write of one document before an earlier write of another, and
// when that earlier write then fails on the index, the ordered retry replays the source events from a state the
// source never had and may never converge.
type bulkSplitter struct {
	fold bool

	bulks   [][]mongo_driver.WriteModel
	current []mongo_driver.WriteModel
	index   map[string]int // index maps a document _id (string form) to its write in current.
	folded  int
}

func newBulkSplitter(fold bool) bulkSplitter {
	return bulkSplitter{
		fold:    fold,
		bulks:   [][]mongo_driver.WriteModel{},
		current: []mongo_driver.WriteModel{},
		index:   map[string]int{},
		folded:  0,
	}
}

// Add appends a write of the document id. An isolated write gets a bulk of its own.
func (b *bulkSplitter) Add(operation mongo_driver.WriteModel, id documentID, isolated bool) {
	if isolated {
		b.flush()
		b.bulks = append(b.bulks, []mongo_driver.WriteModel{operation})
		return
	}
	if pos, ok := b.index[id.String]; ok {
		if b.fold {
			if folded := supersedingWrite(b.current[pos], operation); folded != nil {
				b.current[pos] = folded
				b.folded++
				return
			}
		}
		b.flush()
	}
	b.index[id.String] = len(b.current)
	b.current = append(b.current, operation)
}

// Get returns the bulks in the order they must be applied.
func (b *bulkSplitter) Get() [][]mongo_driver.WriteModel {
	b.flush()
	return b.bulks
}

// Folded is the number of writes folded into an earlier write of the same document.
func (b *bulkSplitter) Folded() int {
	return b.folded
}

func (b *bulkSplitter) flush() {
	if len(b.current) == 0 {
		return
	}
	b.bulks = append(b.bulks, b.current)
	b.current = []mongo_driver.WriteModel{}
	b.index = map[string]int{}
}

// supersedingWrite returns the write to keep in place of prev, an earlier write of the same document, when next
// supersedes it, or nil when the two cannot be folded into one. A delete and a full-document upsert leave nothing
// of what came before, so they stand alone. An insert becomes the replace-upsert of its document: the earlier write
// may have created the document already, and a plain insert would then fail as a duplicate. A partial update
// ($set/$unset) depends on the state left by the earlier write and is not folded. Neither is an upsert over a
// delete when its filter has more than _id: the document may be recreated under another shard key, and the upsert
// by the new key would leave the copy on the old shard in place.
func supersedingWrite(prev, next mongo_driver.WriteModel) mongo_driver.WriteModel {
	switch write := next.(type) {
	case *mongo_driver.DeleteOneModel:
		return next
	case *mongo_driver.ReplaceOneModel:
		if write.Upsert == nil || !*write.Upsert {
			return nil
		}
		if _, deleted := prev.(*mongo_driver.DeleteOneModel); deleted && !filtersByIDOnly(write.Filter) {
			return nil
		}
		return next
	case *mongo_driver.InsertOneModel:
		return insertToUpsertFallback(next)
	default:
		return nil
	}
}
