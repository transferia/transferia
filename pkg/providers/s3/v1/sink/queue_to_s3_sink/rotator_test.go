package queue_to_s3_sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

var (
	startTime        = time.Date(2026, time.February, 28, 10, 9, 8, 7, time.UTC)
	rotationInterval = time.Hour
)

func rawItem(writeTime time.Time) abstract.ChangeItem {
	return abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		writeTime,
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
}

func requireShouldRotate(t *testing.T, rotator Rotator, item *abstract.ChangeItem, expected bool, msgAndArgs ...interface{}) {
	t.Helper()
	shouldRotate, err := rotator.ShouldRotate(item)
	require.NoError(t, err)
	require.Equal(t, expected, shouldRotate, msgAndArgs...)
}

// defaultDestination builds a destination with the default (non time based) partitioner
func defaultDestination(interval time.Duration) *s3_v1_model.S3Destination {
	return &s3_v1_model.S3Destination{
		SerializerType:    model.ParsingFormatJSON,
		Serializer:        s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		RotatorType:       s3_v1_model.DefaultRotator,
		RotatorConfig:     s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: interval}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
}

func newTestRotator(t *testing.T, cfg *s3_v1_model.S3Destination) *DefaultRotator {
	t.Helper()
	rotator, ok := NewRotator(cfg.GetRotator(), NewPartitioner(cfg)).(*DefaultRotator)
	require.True(t, ok)
	return rotator
}

func TestDefaultRotator(t *testing.T) {
	firstItem := rawItem(startTime)

	rotator := newTestRotator(t, defaultDestination(rotationInterval))

	requireShouldRotate(t, rotator, &firstItem, true) // First ShouldRotate is always true
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, startTime.Add(rotationInterval), rotator.nextRotate)

	secondItem := rawItem(startTime.Add(rotationInterval - time.Nanosecond))
	requireShouldRotate(t, rotator, &secondItem, false)

	lastItem := rawItem(startTime.Add(rotationInterval))
	requireShouldRotate(t, rotator, &lastItem, true)
	require.NoError(t, rotator.UpdateState(&lastItem))
	require.Equal(t, startTime.Add(rotationInterval*2), rotator.nextRotate)
}

func TestDefaultRotatorLongIntervals(t *testing.T) {
	firstItem := rawItem(startTime)

	rotator := newTestRotator(t, defaultDestination(rotationInterval))

	requireShouldRotate(t, rotator, &firstItem, true)
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, startTime.Add(rotationInterval), rotator.nextRotate)

	secondItem := rawItem(startTime.Add(rotationInterval * 2))
	requireShouldRotate(t, rotator, &secondItem, true)
	require.NoError(t, rotator.UpdateState(&secondItem))
	require.Equal(t, startTime.Add(rotationInterval*3), rotator.nextRotate)
}

// With the default partitioner Dir never changes, so crossing a day boundary inside
// the rotation interval must not trigger a rotation.
func TestDefaultRotatorDirNeverChangesForDefaultPartitioner(t *testing.T) {
	rotator := newTestRotator(t, defaultDestination(24*time.Hour))

	firstItemTime := time.Date(2026, time.February, 11, 23, 0, 0, 0, time.UTC)
	firstItem := rawItem(firstItemTime)
	requireShouldRotate(t, rotator, &firstItem, true)
	require.NoError(t, rotator.UpdateState(&firstItem))

	nextDayItem := rawItem(firstItemTime.Add(time.Hour))
	requireShouldRotate(t, rotator, &nextDayItem, false,
		"without time based partitioning, crossing a day boundary must not force rotation")
}

// With the time based partitioner an item belonging to another directory has to start a
// new file even though the rotation interval has not elapsed yet.
func TestDefaultRotatorDirConsistency(t *testing.T) {
	cfg := defaultDestination(24 * time.Hour)
	cfg.PartitionerType = s3_v1_model.TimeBasedPartitioner
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{
		TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{PartitionType: s3_v1_model.TimePartitionDay},
	}
	rotator := newTestRotator(t, cfg)

	firstItemTime := time.Date(2026, time.February, 11, 23, 0, 0, 0, time.UTC)
	firstItem := rawItem(firstItemTime)

	requireShouldRotate(t, rotator, &firstItem, true) // first call is always true
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, topic+"/2026/02/11", rotator.currentDir)

	// Crosses midnight into the next day's directory, well within the 24h interval
	nextDayItem := rawItem(firstItemTime.Add(time.Hour))
	requireShouldRotate(t, rotator, &nextDayItem, true,
		"a February 12th record must not be allowed into the February 11th directory just because the interval has not elapsed")

	require.NoError(t, rotator.UpdateState(&nextDayItem))
	require.Equal(t, topic+"/2026/02/12", rotator.currentDir)

	// Same directory, still well within the interval -> no rotation needed
	sameDayLaterItem := rawItem(firstItemTime.Add(6 * time.Hour))
	requireShouldRotate(t, rotator, &sameDayLaterItem, false)
}

// Daily buckets in a non UTC zone roll over at local midnight, not at UTC midnight.
func TestDefaultRotatorDirConsistencyInTimezone(t *testing.T) {
	cfg := defaultDestination(24 * time.Hour)
	cfg.PartitionerType = s3_v1_model.TimeBasedPartitioner
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{
		TimeBased: &s3_v1_model.TimeBasedPartitionerConfig{PartitionType: s3_v1_model.TimePartitionDay, Timezone: "Asia/Tokyo"},
	}
	rotator := newTestRotator(t, cfg)

	// 23:00 UTC on the 11th is already 08:00 on the 12th in Tokyo
	firstItemTime := time.Date(2026, time.February, 11, 23, 0, 0, 0, time.UTC)
	firstItem := rawItem(firstItemTime)

	requireShouldRotate(t, rotator, &firstItem, true) // first call is always true
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, topic+"/2026/02/12", rotator.currentDir)

	// Crosses UTC midnight but stays inside the same Tokyo day -> no rotation
	utcNextDayItem := rawItem(firstItemTime.Add(2 * time.Hour))
	requireShouldRotate(t, rotator, &utcNextDayItem, false,
		"crossing UTC midnight must not roll the file when the bucket is rendered in Asia/Tokyo")

	// 15:00 UTC on the 12th is local midnight of the 13th in Tokyo
	localNextDayItem := rawItem(firstItemTime.Add(16 * time.Hour))
	requireShouldRotate(t, rotator, &localNextDayItem, true)
	require.NoError(t, rotator.UpdateState(&localNextDayItem))
	require.Equal(t, topic+"/2026/02/13", rotator.currentDir)
}
