//go:build !disable_clickhouse_provider

package httpuploader

import "bytes"

type readerStats struct {
	sample     bytes.Buffer
	sampleSize int
	len        int
}

func (q *readerStats) Write(p []byte) (n int, err error) {
	pLen := len(p)
	if needAppend := q.sampleSize - q.sample.Len(); needAppend > 0 {
		if needAppend > pLen {
			needAppend = pLen
		}
		q.sample.Write(p[:needAppend])
	}
	q.len += pLen
	return pLen, nil
}

func (q *readerStats) Len() int {
	return q.len
}

func (q *readerStats) Sample() string {
	res := make([]byte, q.sample.Len())
	copy(res, q.sample.Bytes())
	return string(res) + "...DATA_TRANSFER_TRUNCATED"
}

func newReaderStats(sampleSize int) *readerStats {
	return &readerStats{
		sample:     bytes.Buffer{},
		sampleSize: sampleSize,
		len:        0,
	}
}
