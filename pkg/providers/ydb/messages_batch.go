//go:build !disable_ydb_provider

package ydb

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type batchWithSize struct {
	ydbBatch *topicreader.Batch

	totalSize     uint64
	messageValues [][]byte
}

func newBatchWithSize(batch *topicreader.Batch) (batchWithSize, error) {
	var totalSize uint64
	values := make([][]byte, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		buf := new(bytes.Buffer)
		size, err := buf.ReadFrom(msg)
		if err != nil {
			return batchWithSize{}, err
		}

		totalSize += uint64(size)
		values = append(values, buf.Bytes())
	}

	return batchWithSize{
		ydbBatch:      batch,
		totalSize:     totalSize,
		messageValues: values,
	}, nil
}
