package dblog

import (
	"context"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/maps"
)

type IncrementalAsyncSink struct {
	ctx    context.Context
	logger log.Logger

	signalTable SignalTable

	tableID       abstract.TableID // tableID of transferring table
	tableIterator *IncrementalIterator

	inWindow        bool
	isLastIncrement bool

	primaryKey []string

	chunk         map[string]abstract.ChangeItem
	itemConverter ChangeItemConverter
	stopCallback  func()
	outputPusher  abstract.Pusher
}

func NewIncrementalAsyncSink(
	ctx context.Context,
	logger log.Logger,
	signalTable SignalTable,
	table abstract.TableID,
	tableIterator *IncrementalIterator,
	primaryKey []string,
	chunk map[string]abstract.ChangeItem,
	itemConverter ChangeItemConverter,
	stopCallback func(),
	outputPusher abstract.Pusher,
) *IncrementalAsyncSink {
	asyncSink := &IncrementalAsyncSink{
		ctx:             ctx,
		logger:          logger,
		signalTable:     signalTable,
		tableID:         table,
		tableIterator:   tableIterator,
		inWindow:        false,
		isLastIncrement: false,
		primaryKey:      primaryKey,
		chunk:           chunk,
		itemConverter:   itemConverter,
		stopCallback:    stopCallback,
		outputPusher:    outputPusher,
	}

	return asyncSink
}

func (s *IncrementalAsyncSink) Close() error {
	return nil
}

func (s *IncrementalAsyncSink) isExpectedWatermarkOfType(watermarkType WatermarkType) bool {
	if s.inWindow {
		return watermarkType == HighWatermarkType
	} else {
		return watermarkType == LowWatermarkType
	}
}

func (s *IncrementalAsyncSink) expectedUUID() uuid.UUID {
	if s.inWindow {
		return s.tableIterator.HighWatermarkUUID
	} else {
		return s.tableIterator.LowWatermarkUUID
	}
}

func (s *IncrementalAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	lastUnfilledItemIdx := 0

	for idx, item := range items {
		if item.Table == "__consumer_keeper" {
			continue
		}

		if ok, watermarkType := s.signalTable.IsWatermark(&item, s.tableID, s.expectedUUID()); ok {
			s.logger.Infof("watermark found: %s", watermarkType)

			if !s.isExpectedWatermarkOfType(watermarkType) {
				continue
			}

			if s.inWindow {
				s.inWindow = false

				if err := s.pushChunk(); err != nil {
					return util.MakeChanWithError(err)
				}

				if s.isLastIncrement {
					if err := s.shiftRemainingItems(items, lastUnfilledItemIdx, idx); err != nil {
						return util.MakeChanWithError(err)
					}
					s.stopCallback()
					return util.MakeChanWithError(nil)
				}

				chunk, err := s.tableIterator.Next(s.ctx)
				if err != nil {
					return util.MakeChanWithError(err)
				}

				s.chunk, err = ResolveChunkMapFromArr(chunk, s.primaryKey, s.itemConverter)
				if err != nil {
					return util.MakeChanWithError(err)
				}

				if len(s.chunk) == 0 {
					s.isLastIncrement = true
				}

			} else {
				s.inWindow = true
			}

		} else {
			if abstract.IsSystemTable(item.Table) {
				s.logger.Infof("skipping push of system table event: %s", item.Table)
				continue
			}

			items[lastUnfilledItemIdx] = items[idx]
			lastUnfilledItemIdx++

			if item.TableID() != s.tableID || !s.inWindow {
				continue
			}

			keyValue, err := PKeysToStringArr(&item, s.primaryKey, s.itemConverter)
			if err != nil {
				return util.MakeChanWithError(err)
			}

			encodedKey := stringArrToString(keyValue, defaultSeparator)

			if _, ok = s.chunk[encodedKey]; ok {
				s.logger.Infof("found primary key from chunk: %s", keyValue)
				delete(s.chunk, encodedKey)
			}
		}
	}

	if err := s.pushItems(items, lastUnfilledItemIdx); err != nil {
		return util.MakeChanWithError(err)
	}

	return util.MakeChanWithError(nil)
}

func (s *IncrementalAsyncSink) pushChunk() error {
	if err := s.outputPusher(maps.Values(s.chunk)); err != nil {
		return xerrors.Errorf("failed to push chunk: %w", err)
	}

	if _, err := s.signalTable.CreateWatermark(s.ctx, s.tableID, SuccessWatermarkType, s.tableIterator.lowBound); err != nil {
		return xerrors.Errorf("failed to create success watermark: %w", err)
	}

	return nil
}

func (s *IncrementalAsyncSink) shiftRemainingItems(items []abstract.ChangeItem, lastFilledIdx, curIdx int) error {
	for ; curIdx < len(items); curIdx++ {
		curTableName := items[curIdx].Table
		if abstract.IsSystemTable(curTableName) {
			continue
		}

		items[lastFilledIdx] = items[curIdx]
		lastFilledIdx++
	}

	return s.pushItems(items, lastFilledIdx)
}

func (s *IncrementalAsyncSink) pushItems(items []abstract.ChangeItem, size int) error {
	items = items[:size]

	return s.outputPusher(items)
}
