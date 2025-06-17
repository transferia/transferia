//go:build !disable_yt_provider

package lfstaging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestChangeitems(t *testing.T) {
	ts, err := time.Parse(time.RFC3339, "2022-01-01T01:01:01Z")
	require.NoError(t, err, "Cannot parse time")

	table := "some-table"
	topic := "some-topic"
	shard := 10
	offset := int64(15)
	data := []byte{1, 2, 3}

	ci := abstract.MakeRawMessage(table, ts, topic, shard, offset, data)

	msg, err := GetRawMessage(ci)
	require.NoError(t, err, "GetRawMessage throws")

	require.Equal(
		t,
		msg,
		RawMessage{
			Table:     table,
			Topic:     topic,
			Partition: int32(shard),
			SeqNo:     offset,
			WriteTime: ts,
			Data:      data,
		},
	)
}
