//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

type partitionState struct {
	nextOffset                           int64
	firstOffset                          int64
	nextOffsetWriteTimestampLowerBoundMs int64
}

type logbrokerMetadata struct {
	topic           string
	partitionStates map[int64]*partitionState
}

type serializedLogbrokerMetadataPartition struct {
	NextOffset                           int64 `yson:"next_offset"`
	FirstOffset                          int64 `yson:"first_offset"`
	Partition                            int64 `yson:"partition"`
	NextOffsetWriteTimestampLowerBoundMs int64 `yson:"next_offset_write_timestamp_lower_bound_ms"`
}

type serializedLogbrokerMetadataTopic struct {
	Cluster                  string                                 `yson:"cluster"`
	Topic                    string                                 `yson:"topic"`
	LogbrokerSyncTimestampMs int64                                  `yson:"logbroker_sync_timestamp_ms"`
	LastStepWithTopicsTable  int64                                  `yson:"last_step_with_topics_table"`
	LbPartitions             []interface{}                          `yson:"lb_partitions"`
	Partitions               []serializedLogbrokerMetadataPartition `yson:"partitions"`
}

type serializedLogbrokerMetadata struct {
	Topics []serializedLogbrokerMetadataTopic `yson:"topics"`
}

func newLogbrokerMetadata() *logbrokerMetadata {
	return &logbrokerMetadata{
		topic:           "",
		partitionStates: make(map[int64]*partitionState),
	}
}

func deserializeLogbrokerMetadata(attr *serializedLogbrokerMetadata) (*logbrokerMetadata, error) {
	if len(attr.Topics) != 1 {
		return nil, xerrors.Errorf("'topics' contains more than one topic")
	}

	metadata := newLogbrokerMetadata()
	metadata.topic = attr.Topics[0].Topic

	for _, partition := range attr.Topics[0].Partitions {
		metadata.partitionStates[partition.Partition] = &partitionState{
			nextOffset:                           partition.NextOffset,
			firstOffset:                          partition.FirstOffset,
			nextOffsetWriteTimestampLowerBoundMs: partition.NextOffsetWriteTimestampLowerBoundMs,
		}
	}

	return metadata, nil
}

func lbMetaFromTableAttr(client yt.CypressClient, tablePath ypath.Path) (*logbrokerMetadata, error) {
	var serialized serializedLogbrokerMetadata

	err := client.GetNode(context.TODO(), tablePath.Child("@_logbroker_metadata"), &serialized, nil)
	if err != nil {
		return nil, xerrors.Errorf("Cannot get table attr: %w", err)
	}

	return deserializeLogbrokerMetadata(&serialized)
}

func (lm *logbrokerMetadata) AddIntermediateRow(row intermediateRow) {
	if lm.topic == "" {
		lm.topic = row.TopicName
	}

	partition, ok := lm.partitionStates[row.Shard]
	if !ok {
		lm.partitionStates[row.Shard] = &partitionState{
			nextOffset:                           row.Offset + 1,
			firstOffset:                          row.Offset,
			nextOffsetWriteTimestampLowerBoundMs: row.CommitTimestampMs,
		}
	} else {
		if row.Offset < partition.firstOffset {
			partition.firstOffset = row.Offset
		}

		if row.Offset+1 > partition.nextOffset {
			partition.nextOffset = row.Offset + 1
		}

		if row.CommitTimestampMs > partition.nextOffsetWriteTimestampLowerBoundMs {
			partition.nextOffsetWriteTimestampLowerBoundMs = row.CommitTimestampMs
		}
	}
}

func (lm *logbrokerMetadata) Merge(other *logbrokerMetadata) error {
	if lm.topic == "" {
		lm.topic = other.topic
	}

	for p, v := range other.partitionStates {
		partition, ok := lm.partitionStates[p]
		if !ok {
			lm.partitionStates[p] = v
		} else {
			if v.firstOffset < partition.firstOffset {
				partition.firstOffset = v.firstOffset
			}

			if v.nextOffset > partition.nextOffset {
				partition.nextOffset = v.nextOffset
			}

			if v.nextOffsetWriteTimestampLowerBoundMs < partition.nextOffsetWriteTimestampLowerBoundMs {
				partition.nextOffsetWriteTimestampLowerBoundMs = v.nextOffsetWriteTimestampLowerBoundMs
			}
		}
	}

	return nil
}

func (lm *logbrokerMetadata) serialize() *serializedLogbrokerMetadata {
	serialized := &serializedLogbrokerMetadata{
		Topics: []serializedLogbrokerMetadataTopic{
			{
				Cluster:                  "fakecluster",
				Topic:                    lm.topic,
				LogbrokerSyncTimestampMs: time.Now().UnixMilli(),
				LastStepWithTopicsTable:  0,
				LbPartitions:             []interface{}{},
				Partitions:               []serializedLogbrokerMetadataPartition{},
			},
		},
	}

	for p, partition := range lm.partitionStates {
		serialized.Topics[0].Partitions = append(serialized.Topics[0].Partitions, serializedLogbrokerMetadataPartition{
			NextOffset:                           partition.nextOffset,
			FirstOffset:                          partition.firstOffset,
			NextOffsetWriteTimestampLowerBoundMs: partition.nextOffsetWriteTimestampLowerBoundMs,
			Partition:                            p,
		})
	}

	return serialized
}

func (lm *logbrokerMetadata) saveIntoTableAttr(tx yt.Tx, tablePath ypath.Path) error {
	serialized := lm.serialize()
	return tx.SetNode(context.TODO(), tablePath.Child("@_logbroker_metadata"), serialized, nil)
}
