package logbroker

import (
	"encoding/binary"

	"github.com/transferia/transferia/pkg/parsers"
)

func BatchStatistics(batches []parsers.MessageBatch) (size int64, count int64) {
	for _, batch := range batches {
		for _, m := range batch.Messages {
			count += 1
			size += int64(binary.Size(m.Value))
		}
	}
	return size, count
}
