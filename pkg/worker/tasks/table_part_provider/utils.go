package table_part_provider

import (
	"context"
	"encoding/json"
	"math"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

const (
	asyncPartsDefaultBatchSize = 1000
)

// asyncLoadParts loads leastParts from sourceProvider to sharedMemory.
// If singleWorkerTPP is used (not nil), all leastParts are also Appended to it and its Close() method is deferredly called.
func asyncLoadParts(
	ctx context.Context,
	storage abstract.NextArrTableDescriptionGetterBuilder,
	inTables []abstract.TableDescription,
	sharedMemory abstract.SharedMemory,
	operationID string,
) error {
	logger.Log.Info("Starting async load leastParts with table_part_provider")

	asyncTPPStorage, err := storage.BuildNextArrTableDescriptionGetter(inTables)
	if err != nil {
		return xerrors.Errorf("unable to create async leastParts provider, err: %w", err)
	}
	defer asyncTPPStorage.Close()

	tablePartIndex := make(map[abstract.TableID]uint64, len(inTables))  // Count of already received parts per table.
	tablePartsCount := make(map[abstract.TableID]uint64, len(inTables)) // Count of all parts per table.
	for _, table := range inTables {
		tid := table.ID()
		tablePartIndex[tid] = 1
		tablePartsCount[tid] = math.MaxInt32 // TODO: TM-9427.
	}
	for {
		currTables, err := asyncTPPStorage.NextArrTableDescription(ctx, asyncPartsDefaultBatchSize)
		if err != nil {
			return xerrors.Errorf("unable to async get table desc, err: %w", err)
		}
		if len(currTables) == 0 {
			break
		}

		parts := make([]*abstract.OperationTablePart, 0, len(currTables))
		for _, table := range currTables {
			part := abstract.NewOperationTablePartFromDescription(operationID, &table)
			tid := table.ID()
			part.PartsCount = tablePartsCount[tid]
			part.PartIndex = tablePartIndex[tid]
			tablePartIndex[tid]++
			parts = append(parts, part)
		}
		err = sharedMemory.Store(parts)
		if err != nil {
			return xerrors.Errorf("unable to store current tables, err: %w", err)
		}
	}
	logger.Log.Info("Async load leastParts finished successfully")
	return nil
}

// addKeyToJson unmarshals jsonStr to map[string]any, adds key with value and returns marshalled json.
func addKeyToJson(jsonStr, key string, value any) ([]byte, error) {
	dict := make(map[string]any)
	if jsonStr != "" {
		if err := jsonx.Unmarshal([]byte(jsonStr), &dict); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal JSON string, err: %w", err)
		}
	}
	dict[key] = value
	return json.Marshal(dict)
}
