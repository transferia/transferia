package dblog

import (
	"context"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog/tablequery"
	"go.ytsaurus.tech/library/go/core/log"
)

type IncrementalIterator struct {
	logger log.Logger

	storage     tablequery.TableQueryable
	tableQuery  *tablequery.TableQuery
	signalTable SignalTable

	itemConverter ChangeItemConverter
	pkColNames    []string
	lowBound      []string
	limit         uint64

	LowWatermarkUUID  uuid.UUID
	HighWatermarkUUID uuid.UUID

	betweenMarksOpts []func()
}

func NewIncrementalIterator(
	logger log.Logger,
	storage tablequery.TableQueryable,
	tableQuery *tablequery.TableQuery,
	signalTable SignalTable,
	itemConverter ChangeItemConverter,
	pkColNames []string,
	lowBound []string,
	limit uint64,
	betweenMarksOpts ...func(),
) (*IncrementalIterator, error) {
	iter := &IncrementalIterator{
		logger:            logger,
		storage:           storage,
		tableQuery:        tableQuery,
		signalTable:       signalTable,
		itemConverter:     itemConverter,
		pkColNames:        pkColNames,
		lowBound:          lowBound,
		limit:             limit,
		LowWatermarkUUID:  uuid.New(),
		HighWatermarkUUID: uuid.New(),
		betweenMarksOpts:  betweenMarksOpts,
	}

	return iter, nil
}

func (i *IncrementalIterator) Next(ctx context.Context) ([]abstract.ChangeItem, error) {
	i.tableQuery.Filter = MakeNextWhereStatement(i.pkColNames, i.lowBound)
	i.logger.Infof("IncrementalIterator::Next - i.tableQuery.Filter: %s", i.tableQuery.Filter)
	return i.loadTablePart(ctx)
}

func (i *IncrementalIterator) loadTablePart(ctx context.Context) ([]abstract.ChangeItem, error) {
	lowWatermarkUUID, err := i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, LowWatermarkType, i.lowBound)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create watermark when selecting chunk: %w", err)
	}

	i.logger.Infof("created low watermark, uuid: %s", lowWatermarkUUID.String())

	i.LowWatermarkUUID = lowWatermarkUUID

	chunk := make([]abstract.ChangeItem, 0, i.limit)

	chunkPusher := func(items []abstract.ChangeItem) error {
		if len(items) > 0 {
			lastItem := items[len(items)-1]
			lastKeyValue, err := PKeysToStringArr(&lastItem, i.pkColNames, i.itemConverter)
			if err != nil {
				return xerrors.Errorf("unable to get key value: %w", err)
			}

			i.lowBound = lastKeyValue
		}

		chunk = append(chunk, items...)

		return nil
	}

	if err := i.storage.LoadQueryTable(ctx, *i.tableQuery, chunkPusher); err != nil {
		return nil, xerrors.Errorf("unable to load table: %s, err: %w", i.tableQuery.TableID, err)
	}

	if len(chunk) != 0 {
		for _, opt := range i.betweenMarksOpts {
			opt()
		}
	}

	highWatermarkUUID, err := i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, HighWatermarkType, i.lowBound)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create watermark when selecting chunk")
	}

	i.logger.Infof("created high watermark, uuid: %s", highWatermarkUUID.String())

	i.HighWatermarkUUID = highWatermarkUUID

	return chunk, nil
}
