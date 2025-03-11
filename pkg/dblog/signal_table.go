package dblog

import (
	"context"

	"github.com/google/uuid"
	"github.com/transferria/transferria/pkg/abstract"
)

type WatermarkType string

const (
	LowWatermarkType     = "L"
	HighWatermarkType    = "H"
	SuccessWatermarkType = "S"
	BadWatermarkType     = "B"
)

// The SignalTable is used to create watermarks in the wal,
// and check whether the read item is a watermark
//
// It is assumed that the low_watermark will be placed before reading data from the table,
// and the high_watermark after reading data from the table
//
// When reading the wal, we have 2 types of watermarks needed to check in wal (low and high):
//   - low - means that we are now just before the moment, when we start reading from the database
//   - high - means that we have completed reading
//
// Also we have 2 types needed to resolve additional problem (success watermark and bad watermark):
//   - success - needed to save the last successfully transferred chunk
//   - bad - it is necessary to mark an invalid watermark type
type SignalTable interface {
	CreateWatermark(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType, lowBound []string) (uuid.UUID, error)
	IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID, markUUID uuid.UUID) (bool, WatermarkType)
}
