package async

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// Measurer calculate the size of items passing through it
func Measurer(logger log.Logger) func(abstract.AsyncSink) abstract.AsyncSink {
	return func(s abstract.AsyncSink) abstract.AsyncSink {
		return newMeasurer(s, logger)
	}
}

type measurer struct {
	sink   abstract.AsyncSink
	logger log.Logger
}

func newMeasurer(s abstract.AsyncSink, logger log.Logger) *measurer {
	return &measurer{
		sink:   s,
		logger: logger,
	}
}

func (m *measurer) Close() error {
	return m.sink.Close()
}

const measurerMinimumLogDuration time.Duration = time.Second

func (m *measurer) AsyncPush(items []abstract.ChangeItem) chan error {
	start := time.Now()
	for i := range items {
		items[i].Size.Values = util.DeepSizeof(items[i].ColumnValues)
	}
	if elapsed := time.Since(start); elapsed > measurerMinimumLogDuration {
		var totalValuesSize uint64
		var totalReadSize uint64
		for _, i := range items {
			totalValuesSize += i.Size.Values
			totalReadSize += i.Size.Read
		}
		m.logger.Info(
			fmt.Sprintf("items size measurer took more than %s to calculate items' size", measurerMinimumLogDuration.String()),
			log.Int("len", len(items)),
			log.String("total_read_size", humanize.Bytes(totalReadSize)),
			log.String("total_values_size", humanize.Bytes(totalValuesSize)),
			log.Duration("elapsed", elapsed),
			log.String("values_size_per_second", humanize.Bytes(uint64(float64(totalValuesSize)/elapsed.Seconds()))),
		)
	}
	return m.sink.AsyncPush(items)
}
