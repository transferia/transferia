package tasks

import (
	"fmt"
	sync "sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type loadProgress struct {
	sink        abstract.Sinker
	lastReport  time.Time
	part        *model.OperationTablePart
	workerIndex int
}

func NewLoadProgress(workerIndex int, part *model.OperationTablePart, progressUpdateMutex *sync.Mutex) *loadProgress {
	return &loadProgress{
		sink:        nil,
		lastReport:  time.Now(),
		part:        part,
		workerIndex: workerIndex,
	}
}

func (l *loadProgress) SinkOption() abstract.SinkOption {
	return func(sinker abstract.Sinker) abstract.Sinker {
		l.sink = sinker
		return l
	}
}

func (l *loadProgress) Close() error {
	return l.sink.Close()
}

func (l *loadProgress) Push(items []abstract.ChangeItem) error {
	l.reportProgress()

	if err := l.sink.Push(items); err != nil {
		return err
	}

	l.updateProgress(items)
	return nil
}

func (l *loadProgress) updateProgress(items []abstract.ChangeItem) {
	rowEvents := uint64(0)
	readBytes := uint64(0)
	for i := range items {
		if !items[i].IsRowEvent() {
			continue
		}
		rowEvents += 1
		readBytes += items[i].Size.Read
	}

	l.part.CompletedRows += rowEvents
	l.part.ReadBytes += readBytes
}

func (l *loadProgress) reportProgress() {
	now := time.Now()
	if now.Sub(l.lastReport) > time.Second*15 {
		l.lastReport = now
		logger.Log.Info(
			fmt.Sprintf("Load table '%v' progress %v / %v (%.2f%%)", l.part, l.part.CompletedRows, l.part.ETARows, l.part.CompletedPercent()),
			log.String("table_part", l.part.StringWithoutFilter()), log.Int("worker_index", l.workerIndex))
	}
}
