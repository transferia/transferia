//go:build !disable_yt_provider

package lfstaging

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

type intermediateRow struct {
	TopicName         string `yson:"topic_name"`
	SourceURI         string `yson:"source_uri"`
	SourceID          string `yson:"source_id"`
	CommitTimestampMs int64  `yson:"commit_timestamp_ms"`
	Offset            int64  `yson:"offset"`
	Shard             int64  `yson:"shard"`
	Data              []byte `yson:"data"`
}

type lfStagingRow struct {
	Key    string `yson:"key"`
	Subkey string `yson:"subkey"`
	Value  []byte `yson:"value"`
}

func intermediateRowSchema() (ytschema.Schema, error) {
	return ytschema.Infer(intermediateRow{
		TopicName:         "topic-name",
		SourceURI:         "source-uri",
		SourceID:          "source-id",
		CommitTimestampMs: 10,
		Offset:            10,
		Shard:             10,
		Data:              []byte{},
	})
}

func lfStagingRowSchema() (ytschema.Schema, error) {
	return ytschema.Infer(lfStagingRow{
		Key:    "asdf",
		Subkey: "asdf",
		Value:  []byte{},
	})
}

func intermediateRowFromChangeItem(ci abstract.ChangeItem) (intermediateRow, error) {
	if !ci.IsMirror() {
		return intermediateRow{}, xerrors.Errorf("TableSchema should be equal to RawDataSchema")
	}

	message, err := GetRawMessage(ci)
	if err != nil {
		return intermediateRow{}, xerrors.Errorf("LfStaging - Could not rebuild raw message: %w", err)
	}

	namespacedTopicName := "data-transfer/" + message.Topic

	return intermediateRow{
		TopicName:         namespacedTopicName,
		SourceURI:         "data-transfer",
		SourceID:          "example-dt-source-id",
		CommitTimestampMs: int64(message.WriteTime.UnixMilli()),
		Offset:            int64(message.SeqNo),
		Shard:             int64(message.Partition),
		Data:              message.Data,
	}, nil
}

func lfStagingRowFromIntermediate(row intermediateRow) lfStagingRow {
	commitTimestamp := time.UnixMilli(row.CommitTimestampMs)

	topicParts := strings.Split(row.TopicName, "/")
	logName := topicParts[len(topicParts)-1]
	oldStyleTopic := strings.Join(topicParts, "--")

	key := fmt.Sprintf("%v %v", row.SourceURI, commitTimestamp.Format("2006-01-02 15:03:04"))
	subkey := fmt.Sprintf(
		"%v@@%v@@%v@@%v@@%v@@%v@@%v@@%v",
		fmt.Sprintf("fakecluster--%v:%v", oldStyleTopic, row.Shard),
		row.Offset,
		row.SourceID,
		row.CommitTimestampMs,
		row.CommitTimestampMs/1000,
		logName,
		row.Offset,
		row.CommitTimestampMs,
	)

	return lfStagingRow{
		Key:    key,
		Subkey: subkey,
		Value:  row.Data,
	}
}
