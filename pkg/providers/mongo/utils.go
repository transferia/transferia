package mongo

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
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
	partID := table.GeneratePartID()
	inflight := make([]abstract.ChangeItem, 0)
	globalIdx := uint64(0)
	byteSize := uint64(0)
	for cursor.Next(ctx) {
		var item bson.D
		if err := cursor.Decode(&item); err != nil {
			if errors.Is(err, bsoncore.ErrInvalidLength) || strings.Contains(err.Error(), "invalid length") {
				return coded.Errorf(codes.MongoInvalidDeprecatedBinarySubtype, "cursor.Decode returned error: %w", err)
			}
			return xerrors.Errorf("cursor.Decode returned error: %w", err)
		}

		extItem := DExt(item)
		id, err := ExtractKey(extItem.Map()["_id"], s.IsHomo)
		if err != nil {
			return xerrors.Errorf("cannot extract key: %w", err)
		}
		val := extItem.Value(s.IsHomo, s.preventJSONRepack)
		changeItem := abstract.ChangeItem{
			ID:               0,
			LSN:              0,
			CommitTime:       uint64(st.UnixNano()),
			Counter:          0,
			Kind:             abstract.InsertKind,
			Schema:           table.Schema,
			Table:            table.Name,
			PartID:           partID,
			ColumnNames:      DocumentSchema.ColumnsNames,
			ColumnValues:     []interface{}{id, val},
			TableSchema:      DocumentSchema.Columns,
			OldKeys:          abstract.EmptyOldKeys(),
			Size:             abstract.RawEventSize(uint64(len(cursor.Current))),
			TxID:             "",
			Query:            "",
			QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
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
