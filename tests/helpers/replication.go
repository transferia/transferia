package helpers

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

// Wait for equals rows count in different DB servers.
func WaitEqualRowsCount(
	t *testing.T,
	schema, tableName string,
	source, destination abstract.Storage,
	maxDuration time.Duration,
) error {
	return WaitEqualRowsCountDifferentTables(
		t,
		schema, tableName,
		schema, tableName,
		source, destination,
		maxDuration)
}

// Wait for equals rows count in different schemas.
// May be used for wait rows count in same DB.
func WaitEqualRowsCountDifferentSchemas(
	t *testing.T,
	sourceSchema, destinationSchema, tableName string,
	source, destination abstract.Storage,
	maxDuration time.Duration,
) error {
	return WaitEqualRowsCountDifferentTables(
		t,
		sourceSchema, tableName,
		destinationSchema, tableName,
		source, destination,
		maxDuration)
}

// Wait for equals rows count in different schemas and tables.
// May be used for wait rows count in same DB, same schema and different tables.
func WaitEqualRowsCountDifferentTables(
	t *testing.T,
	sourceSchema, sourceTableName, destinationSchema, destinationTableName string,
	source, destination abstract.Storage,
	maxDuration time.Duration,
) error {
	sourceTableID := *abstract.NewTableID(sourceSchema, sourceTableName)
	sourceTableRowsCount, err := source.ExactTableRowsCount(sourceTableID)
	require.NoError(t, err)
	logger.Log.Infof("Source table %v rows count: %v", TableIDFullName(sourceTableID), sourceTableRowsCount)

	destinationTableID := *abstract.NewTableID(destinationSchema, destinationTableName)
	if err := WaitDestinationEqualRowsCount(destinationSchema, destinationTableName, destination, maxDuration, sourceTableRowsCount); err != nil {
		return xerrors.Errorf("Error while wait for destination table %v target rows count: %w",
			destinationTableID, err) //TODO: why we need error, if we have t?
	}
	logger.Log.Infof("Source table %v rows count is equal destination table %v rows count: %v",
		TableIDFullName(sourceTableID), TableIDFullName(destinationTableID), sourceTableRowsCount)

	return nil
}

func WaitDestinationEqualRowsCount(
	destinationSchema, destinationTableName string,
	destination abstract.Storage,
	maxDuration time.Duration,
	sourceTableRowsCount uint64,
) error {
	destinationTableID := *abstract.NewTableID(destinationSchema, destinationTableName)
	logger.Log.Infof("Maximum wait for destination table %v target rows count: %v",
		TableIDFullName(destinationTableID), maxDuration)

	sleepTime := time.Second * 2
	startTime := time.Now()
	destinationTableRowsCount := uint64(math.MaxUint64)
	for {
		duration := time.Since(startTime)
		logger.Log.Infof("Wait for destination table %v target rows count: %v",
			TableIDFullName(destinationTableID), duration)
		if duration > maxDuration {
			if destinationTableRowsCount != math.MaxUint64 {
				return xerrors.Errorf(
					"Exceeded max allowed duration for destination table %v target rows count. Have %d rows instead of %d",
					TableIDFullName(destinationTableID), destinationTableRowsCount, sourceTableRowsCount)
			}
			return xerrors.Errorf("Exceeded max allowed duration for destination table %v target rows count",
				TableIDFullName(destinationTableID))
		}

		exists, err := destination.TableExists(destinationTableID)
		if err != nil {
			logger.Log.Warnf("Wait for destination table %v target rows count returned error: %s",
				TableIDFullName(destinationTableID), err)

			time.Sleep(sleepTime)
			continue
		}
		if !exists {
			logger.Log.Warnf("Wait for destination table %v: table not exists",
				TableIDFullName(destinationTableID))

			time.Sleep(sleepTime)
			continue
		}

		destinationTableRowsCount, err = destination.ExactTableRowsCount(destinationTableID)
		if err != nil {
			logger.Log.Warnf("Wait for destination table %v target rows count, get rows count returned error: %s",
				TableIDFullName(destinationTableID), err)
			continue
		}

		logger.Log.Infof("Destination table %v rows count: %v, expect %v",
			TableIDFullName(destinationTableID), destinationTableRowsCount, sourceTableRowsCount)

		if sourceTableRowsCount == destinationTableRowsCount {
			return nil
		}

		time.Sleep(sleepTime)
	}
}

func WaitCond(maxDuration time.Duration, condFunc func() bool) error {
	startTime := time.Now()
	for {
		cond := condFunc()
		logger.Log.Infof("Condition is %v", cond)
		if cond {
			return nil
		}

		duration := time.Since(startTime)
		logger.Log.Infof("Wait for condition")
		if duration > maxDuration {
			return xerrors.Errorf("Exceeded max allowed duration for waiting condition")
		}
		time.Sleep(time.Second)
	}
}
