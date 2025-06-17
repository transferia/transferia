//go:build !disable_yt_provider

package lfstaging

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/xerrors"
)

type RawMessage struct {
	Table     string
	Topic     string
	Partition int32
	SeqNo     int64
	WriteTime time.Time
	Data      []byte
}

func getRawMessageTopic(ci abstract.ChangeItem) (string, error) {
	switch v := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageTopic]].(type) {
	case string:
		return v, nil
	default:
		return "", xerrors.Errorf("Could not get raw topic - invalid type '%t'", v)
	}
}

func getRawMessagePartition(ci abstract.ChangeItem) (int32, error) {
	switch v := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessagePartition]].(type) {
	case uint64:
		return int32(v), nil
	case int64:
		return int32(v), nil
	case int32:
		return v, nil
	case int:
		return int32(v), nil
	default:
		return 0, xerrors.Errorf("Could not get raw shard - invalid type '%t'", v)
	}
}

func getRawMessageSeqNo(ci abstract.ChangeItem) (int64, error) {
	switch v := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageSeqNo]].(type) {
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, xerrors.Errorf("Could not get raw seqNo - invalid type '%t'", v)
	}
}

func getRawMessageWriteTime(ci abstract.ChangeItem) (time.Time, error) {
	switch v := ci.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageWriteTime]].(type) {
	case time.Time:
		return v, nil
	default:
		return time.Time{}, xerrors.Errorf("Could not get raw write time - invalid type '%t'", v)
	}
}

func GetRawMessage(ci abstract.ChangeItem) (RawMessage, error) {
	table := ci.Table

	topic, err := getRawMessageTopic(ci)
	if err != nil {
		return RawMessage{}, xerrors.Errorf("Could not rebuild raw message from changeitem: %w", err)
	}

	partition, err := getRawMessagePartition(ci)
	if err != nil {
		return RawMessage{}, xerrors.Errorf("Could not rebuild raw message from changeitem: %w", err)
	}

	seqNo, err := getRawMessageSeqNo(ci)
	if err != nil {
		return RawMessage{}, xerrors.Errorf("Could not rebuild raw message from changeitem: %w", err)
	}

	writeTime, err := getRawMessageWriteTime(ci)
	if err != nil {
		return RawMessage{}, xerrors.Errorf("Could not rebuild raw message from changeitem: %w", err)
	}

	data, err := abstract.GetRawMessageData(ci)
	if err != nil {
		return RawMessage{}, xerrors.Errorf("Could not rebuild raw message from changeitem: %w", err)
	}

	return RawMessage{
		Table:     table,
		Topic:     topic,
		Partition: partition,
		SeqNo:     seqNo,
		WriteTime: writeTime,
		Data:      data,
	}, nil
}
