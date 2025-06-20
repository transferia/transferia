//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s Storage) readRowsAndPushByChunks(
	ctx context.Context,
	cursor *mongo.Cursor,
	st time.Time,
	table abstract.TableDescription,
	chunkSize uint64,
	chunkByteSize uint64,
	pusher abstract.Pusher,
) error {
	partID := table.PartID()
	inflight := make([]abstract.ChangeItem, 0)
	globalIdx := uint64(0)
	byteSize := uint64(0)
	for cursor.Next(ctx) {
		var item bson.D
		if err := cursor.Decode(&item); err != nil {
			return xerrors.Errorf("cursor.Decode returned error: %w", err)
		}

		extItem := DExt(item)
		id, err := ExtractKey(extItem.Map()["_id"], s.IsHomo)
		if err != nil {
			return xerrors.Errorf("cannot extract key: %w", err)
		}
		val := extItem.Value(s.IsHomo, s.preventJSONRepack)
		changeItem := abstract.ChangeItem{
			CommitTime:   uint64(st.UnixNano()),
			Kind:         abstract.InsertKind,
			Schema:       table.Schema,
			Table:        table.Name,
			PartID:       partID,
			ColumnNames:  DocumentSchema.ColumnsNames,
			ColumnValues: []interface{}{id, val},
			TableSchema:  DocumentSchema.Columns,
			OldKeys:      abstract.EmptyOldKeys(),
			Counter:      0,
			ID:           0,
			LSN:          0,
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(uint64(len(cursor.Current))),
		}
		if !s.IsHomo {
			err := strictify.Strictify(&changeItem, DocumentSchema.Columns.FastColumns())
			if err != nil {
				return abstract.NewFatalError(
					xerrors.Errorf("non strict value in hetero transfer: %w", err),
				)
			}
		}
		inflight = append(inflight, changeItem)
		globalIdx++
		byteSize += uint64(len(cursor.Current))
		s.metrics.ChangeItems.Inc()
		s.metrics.Size.Add(int64(len(cursor.Current)))
		if uint64(len(inflight)) >= chunkSize {
			if err := pusher(inflight); err != nil {
				return xerrors.Errorf("Cannot push documents to the sink: %w", err)
			}
			byteSize = 0
			inflight = make([]abstract.ChangeItem, 0)
		} else if byteSize > chunkByteSize {
			if err := pusher(inflight); err != nil {
				return xerrors.Errorf("Cannot push documents (%d bytes, %d items) to the sink: %w", byteSize, len(inflight), err)
			}
			byteSize = 0
			inflight = make([]abstract.ChangeItem, 0)
		}
	}
	if len(inflight) > 0 {
		if err := pusher(inflight); err != nil {
			return xerrors.Errorf("Cannot push last chunk (%d items) to the sink: %w", len(inflight), err)
		}
	}

	return nil
}
