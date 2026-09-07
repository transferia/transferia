package queue_to_s3_sink

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

const (
	topicMame = "testtopic"
)

func TestPartitionerNoEncoding(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    topicMame,
			Offset:       123,
			PartitionNum: 1,
		},
	}

	serializer := cfg.GetSerializer()
	partitioner := NewPartitioner(cfg)
	path, err := partitioner.ConstructKey(testItem)
	require.NoError(t, err)
	expected := fmt.Sprintf("%s/partition=%d/%s+%d+%d.%s",
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.Offset,
		strings.ToLower(string(serializer.FormatName())),
	)
	require.Equal(t, expected, path)

	_, ok := partitioner.(*DefaultPartitioner)
	require.True(t, ok)
}

func TestPartitionerEncoding(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.GzipEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    topicMame,
			Offset:       123,
			PartitionNum: 1,
		},
	}

	serializer := cfg.GetSerializer()
	partitioner := NewPartitioner(cfg)
	path, err := partitioner.ConstructKey(testItem)
	require.NoError(t, err)
	expected := fmt.Sprintf("%s/partition=%d/%s+%d+%d.%s.gz",
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.TopicName,
		testItem.QueueMessageMeta.PartitionNum,
		testItem.QueueMessageMeta.Offset,
		strings.ToLower(string(serializer.FormatName())),
	)
	require.Equal(t, expected, path)

	_, ok := partitioner.(*DefaultPartitioner)
	require.True(t, ok)
}

func TestPartitionerIncorrectItem(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    "",
			Offset:       123,
			PartitionNum: 1,
		},
	}

	partitioner := NewPartitioner(cfg)
	_, err := partitioner.ConstructKey(testItem)
	require.Error(t, err)
}

func TestTimeBasedPartitioner(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.TimeBasedPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{PartitionType: s3_v1_model.TimePartitionHour}},
	}

	writeTime := time.Date(2026, time.February, 28, 13, 9, 8, 7, time.UTC)
	testItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topicMame,
		writeTime,
		topicMame,
		1,
		int64(123),
		[]byte("stub"),
	)

	serializer := cfg.GetSerializer()
	partitioner := NewPartitioner(cfg)
	path, err := partitioner.ConstructKey(&testItem)
	require.NoError(t, err)

	expected := fmt.Sprintf("%s/2026/02/28/13/%s+%d+%d.%s",
		topicMame,
		topicMame,
		1,
		123,
		strings.ToLower(string(serializer.FormatName())),
	)
	require.Equal(t, expected, path)
}

func TestTimeBasedPartitionerEncoding(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.TimeBasedPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.GzipEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{PartitionType: s3_v1_model.TimePartitionHour}},
	}

	writeTime := time.Date(2026, time.February, 28, 13, 9, 8, 7, time.UTC)
	testItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topicMame,
		writeTime,
		topicMame,
		1,
		int64(123),
		[]byte("stub"),
	)

	serializer := cfg.GetSerializer()
	partitioner := NewPartitioner(cfg)
	path, err := partitioner.ConstructKey(&testItem)
	require.NoError(t, err)

	expected := fmt.Sprintf("%s/2026/02/28/13/%s+%d+%d.%s.gz",
		topicMame,
		topicMame,
		1,
		123,
		strings.ToLower(string(serializer.FormatName())),
	)
	require.Equal(t, expected, path)
}

func TestTimeBasedPartitionerIncorrectItem(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		PartitionerType:   s3_v1_model.TimeBasedPartitioner,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{PartitionType: s3_v1_model.TimePartitionHour}},
	}
	testItem := &changeitem.ChangeItem{
		QueueMessageMeta: changeitem.QueueMessageMeta{
			TopicName:    "",
			Offset:       123,
			PartitionNum: 1,
		},
	}

	partitioner := NewPartitioner(cfg)
	_, err := partitioner.ConstructKey(testItem)
	require.Error(t, err)
}

// The time bucket is rendered in the configured timezone, so a message written at 22:00 UTC
// lands in the next day's directory for a zone that is far enough ahead.
func TestTimeBasedPartitionerTimezone(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:  model.ParsingFormatJSON,
		PartitionerType: s3_v1_model.TimeBasedPartitioner,
		Serializer:      s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{
			PartitionType: s3_v1_model.TimePartitionHour,
			Timezone:      "Asia/Tokyo", // UTC+9, no DST
		}},
	}

	writeTime := time.Date(2026, time.February, 28, 22, 9, 8, 7, time.UTC)
	testItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topicMame,
		writeTime,
		topicMame,
		1,
		int64(123),
		[]byte("stub"),
	)

	serializer := cfg.GetSerializer()
	partitioner := NewPartitioner(cfg)
	path, err := partitioner.ConstructKey(&testItem)
	require.NoError(t, err)

	expected := fmt.Sprintf("%s/2026/03/01/07/%s+%d+%d.%s",
		topicMame,
		topicMame,
		1,
		123,
		strings.ToLower(string(serializer.FormatName())),
	)
	require.Equal(t, expected, path)
}

func TestTimeBasedPartitionerUnknownTimezone(t *testing.T) {
	cfg := &s3_v1_model.S3Destination{
		SerializerType:  model.ParsingFormatJSON,
		PartitionerType: s3_v1_model.TimeBasedPartitioner,
		Serializer:      s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		PartitionerConfig: s3_v1_model.PartitionerUnion{TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{
			PartitionType: s3_v1_model.TimePartitionHour,
			Timezone:      "Mars/Olympus_Mons",
		}},
	}

	// Rejected up front, so a transfer with such a config never reaches the sink
	require.Error(t, cfg.Validate())

	// And should it get through anyway, the partitioner must fail rather than
	// silently fall back to some other zone
	testItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topicMame,
		time.Date(2026, time.February, 28, 22, 9, 8, 7, time.UTC),
		topicMame,
		1,
		int64(123),
		[]byte("stub"),
	)
	_, err := NewPartitioner(cfg).ConstructKey(&testItem)
	require.Error(t, err)
}
