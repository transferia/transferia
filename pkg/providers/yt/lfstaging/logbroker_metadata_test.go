//go:build !disable_yt_provider

package lfstaging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeDefaultMetadata() *logbrokerMetadata {
	meta := newLogbrokerMetadata()
	meta.AddIntermediateRow(intermediateRow{
		TopicName:         "some-topic",
		SourceURI:         "some-source-uri",
		SourceID:          "some-source-id",
		CommitTimestampMs: 12345,
		Offset:            4567,
		Shard:             1,
		Data:              []byte{},
	})
	meta.AddIntermediateRow(intermediateRow{
		TopicName:         "some-topic",
		SourceURI:         "some-source-uri",
		SourceID:          "some-source-id",
		CommitTimestampMs: 23456,
		Offset:            6789,
		Shard:             1,
		Data:              []byte{},
	})

	return meta
}

func TestUpdate(t *testing.T) {
	meta := makeDefaultMetadata()
	require.Equal(t, "some-topic", meta.topic)
	require.Equal(t, int64(4567), meta.partitionStates[1].firstOffset)
	require.Equal(t, int64(6790), meta.partitionStates[1].nextOffset)
	require.Equal(t, int64(23456), meta.partitionStates[1].nextOffsetWriteTimestampLowerBoundMs)
}

func TestSerializeDeserialize(t *testing.T) {
	meta := makeDefaultMetadata()

	serialized := meta.serialize()
	newMeta, err := deserializeLogbrokerMetadata(serialized)

	require.NoError(t, err, "deserializeLogbrokerMetadata throws")

	require.Equal(t, meta.topic, newMeta.topic)
	require.Equal(t, len(meta.partitionStates), len(newMeta.partitionStates))

	for i, p := range meta.partitionStates {
		require.Equal(t, p.firstOffset, newMeta.partitionStates[i].firstOffset)
		require.Equal(t, p.nextOffset, newMeta.partitionStates[i].nextOffset)
		require.Equal(t, p.nextOffsetWriteTimestampLowerBoundMs, newMeta.partitionStates[i].nextOffsetWriteTimestampLowerBoundMs)
	}
}
