package log_miner

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

const (
	maxBatchSize = 50_000
	maxBatchTime = time.Second * 5
)

type logMinerBatch struct {
	Rows             []abstract.ChangeItem
	CreateTime       time.Time
	ProgressPosition *oracle_common.LogPosition
}

func newLogMinerBatch() *logMinerBatch {
	return &logMinerBatch{
		Rows:             []abstract.ChangeItem{},
		CreateTime:       time.Now(),
		ProgressPosition: nil,
	}
}

func (batch *logMinerBatch) Empty() bool {
	return len(batch.Rows) == 0
}

func (batch *logMinerBatch) SetProgressPosition(position *oracle_common.LogPosition) {
	if position == nil {
		return
	}
	batch.ProgressPosition = position
}

func (batch *logMinerBatch) HasProgressPosition() bool {
	return batch.ProgressPosition != nil
}

func (batch *logMinerBatch) Ready() bool {
	return len(batch.Rows) >= maxBatchSize || time.Since(batch.CreateTime) >= maxBatchTime
}

func (batch *logMinerBatch) Add(item abstract.ChangeItem) {
	batch.Rows = append(batch.Rows, item)
}
