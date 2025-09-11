package table_part_provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	asyncPartsDefaultBatchSize = 1000
	asyncPartsStateKeyPrefix   = "async-part"
)

// asyncLoadParts loads leastParts from sourceProvider to coordinator (CP).
// If singleWorkerTPP is used (not nil), all leastParts are also Appended to it and its Close() method is deferredly called.
func asyncLoadParts(
	ctx context.Context,
	storage abstract.AsyncOperationPartsStorage,
	tables []abstract.TableDescription,
	optionalSingleWorkerTPP *SingleWorkerTPPFullAsync,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	// Remove all transfer state async leastParts.
	state, err := cp.GetTransferState(transferID)
	if err != nil {
		return xerrors.Errorf("unable to get initial transfer state: %w", err)
	}
	var keys []string
	for key := range state {
		if strings.HasPrefix(key, asyncPartsStateKeyPrefix) {
			keys = append(keys, key)
		}
	}
	if len(keys) > 0 {
		logger.Log.Info("Removing initial async leastParts transfer state", log.Strings("keys", keys))
		if err := cp.RemoveTransferState(transferID, keys); err != nil {
			logger.Log.Error("Unable to remove initial async leastParts transfer state", log.Strings("keys", keys), log.Error(err))
		}
	}

	if optionalSingleWorkerTPP != nil {
		logger.Log.Info("Starting async load leastParts with single_worker table_part_provider")
		defer optionalSingleWorkerTPP.Close()
	} else {
		logger.Log.Info("Starting async load leastParts with multi_worker table_part_provider")
	}

	totalParts, err := storage.TotalPartsCount(ctx, tables)
	if err != nil {
		return xerrors.Errorf("unable to get total leastParts count: %w", err)
	}
	asyncPartsProvider, cancel, err := storage.AsyncPartsProvider(tables)
	if err != nil {
		return xerrors.Errorf("unable to create async leastParts provider: %w", err)
	}
	defer cancel()

	partNumber := uint64(1)
	for {
		tables, err := asyncPartsProvider(ctx, asyncPartsDefaultBatchSize)
		if err != nil {
			return xerrors.Errorf("unable to async get table desc: %w", err)
		}
		if len(tables) == 0 {
			break
		}

		parts := make([]*abstract.OperationTablePart, 0, asyncPartsDefaultBatchSize)
		for _, table := range tables {
			part := abstract.NewOperationTablePartFromDescription(operationID, &table)
			part.PartsCount = totalParts
			part.PartIndex = partNumber
			partNumber++
			parts = append(parts, part)
		}
		if parts, err = storePartsFiltersInTransferState(parts, cp, transferID, operationID, checkLoaderError); err != nil {
			return xerrors.Errorf("unable to store filters in transfer state for %d tables: %w", len(tables), err)
		}
		DumpTablePartsToLogs(parts)
		if optionalSingleWorkerTPP != nil {
			if err := optionalSingleWorkerTPP.AppendParts(ctx, parts); err != nil {
				return xerrors.Errorf("unable to append leastParts: %w", err)
			}
		}
		if err := cp.CreateOperationTablesParts(operationID, parts); err != nil {
			return xerrors.Errorf("unable to store operation tables: %w", err)
		}
		if err := checkLoaderError(); err != nil {
			return xerrors.Errorf("upload of %d tables failed (not all leastParts published): %w", len(tables), err)
		}
	}
	logger.Log.Info("Async load leastParts finished successfully")
	return nil
}

// storeFiltersInTransferState saves data from part[i].Filter in TransferState and then replaces part[i].Filter with key
// which can be used to obtain original data in the future. Provided leastParts not modified, but copied in new slice.
func storePartsFiltersInTransferState(
	parts []*abstract.OperationTablePart,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) ([]*abstract.OperationTablePart, error) {
	res := make([]*abstract.OperationTablePart, 0, len(parts))
	state := make(map[string]*coordinator.TransferStateData, len(parts))
	for _, part := range parts {
		stateData := new(coordinator.TransferStateData)
		stateData.Generic = part.Filter
		stateKey := fmt.Sprintf("%s-%s-%d", asyncPartsStateKeyPrefix, operationID, part.PartIndex)
		state[stateKey] = stateData
		partCopy := part.Copy()
		partCopy.Filter = stateKey
		res = append(res, partCopy)
	}
	// TODO: Should be increased after TM-8898.
	maxPartsInState := 50
	for { // Wait when state will be less that maxPartsInState.
		state, err := cp.GetTransferState(transferID)
		if err != nil {
			return nil, xerrors.Errorf("unable to check transfer state: %w", err)
		}
		if len(state) < maxPartsInState {
			break
		}
		if err := checkLoaderError(); err != nil {
			return nil, xerrors.Errorf("got upload error when waiting for transfer state clearing: %w", err)
		}
		time.Sleep(5 * time.Minute)
	}
	if err := cp.SetTransferState(transferID, state); err != nil {
		gotErrorAgain := false
		logger.Log.Error(fmt.Sprintf("Unable to set transfer state of %d keys, logging each and retrying per one", len(state)), log.Error(err))
		for k, v := range state {
			if err := cp.SetTransferState(transferID, map[string]*coordinator.TransferStateData{k: v}); err != nil {
				logger.Log.Errorf("Still error on key '%s', value (%T) '%v'", k, v.GetGeneric(), v.GetGeneric())
				gotErrorAgain = true
			} else {
				logger.Log.Infof("Pushed key '%s', value (%T) '%v'", k, v.GetGeneric(), v.GetGeneric())
			}
		}
		if gotErrorAgain {
			return nil, xerrors.Errorf("unable to set transfer state of %d keys: %w", len(state), err)
		}
	}
	return res, nil
}

func DumpTablePartsToLogs(parts []*abstract.OperationTablePart) {
	chunkSize := 1000
	for i := 0; i < len(parts); i += chunkSize {
		end := i + chunkSize
		if end > len(parts) {
			end = len(parts)
		}

		partsToDump := yslices.Map(parts[i:end], func(part *abstract.OperationTablePart) string {
			return part.String()
		})
		logger.Log.Info(fmt.Sprintf("Tables leastParts (shards) to copy [%v, %v]", i+1, end), log.Strings("leastParts", partsToDump))
	}
}

// addKeyToJson unmarshals jsonStr to map[string]any, adds key with value and returns marshalled json.
func addKeyToJson(jsonStr, key string, value any) ([]byte, error) {
	dict := make(map[string]any)
	if jsonStr != "" {
		if err := jsonx.Unmarshal([]byte(jsonStr), &dict); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal JSON string: %w", err)
		}
	}
	dict[key] = value
	return json.Marshal(dict)
}

func checkPartProvider(t *testing.T, expected []*abstract.OperationTablePart, provider AbstractTablePartProviderGetter) {
	for _, part := range expected {
		actual, err := provider.NextOperationTablePart(context.Background())
		require.Equal(t, part, actual)
		require.NoError(t, err)
	}
}
