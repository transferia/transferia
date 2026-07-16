package queue_to_s3_sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

var (
	startTime        = time.Date(2026, time.February, 28, 10, 9, 8, 7, time.UTC)
	rotationInterval = time.Hour
)

func TestDefaultRotator(t *testing.T) {
	firstItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime,
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)

	cfg := &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}
	rotator, ok := NewRotator(cfg).(*DefaultRotator)
	require.True(t, ok)

	require.True(t, rotator.ShouldRotate(&firstItem)) // First ShouldRotate is always true
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, startTime.Add(rotationInterval), rotator.nextRotate)

	secondItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime.Add(rotationInterval-time.Nanosecond),
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	require.False(t, rotator.ShouldRotate(&secondItem))

	lastItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime.Add(rotationInterval),
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	require.True(t, rotator.ShouldRotate(&lastItem))
	require.NoError(t, rotator.UpdateState(&lastItem))
	require.Equal(t, startTime.Add(rotationInterval*2), rotator.nextRotate)
}

func TestDefaultRotatorLongIntervals(t *testing.T) {
	firstItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime,
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)

	cfg := &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}
	rotator, ok := NewRotator(cfg).(*DefaultRotator)
	require.True(t, ok)

	require.True(t, rotator.ShouldRotate(&firstItem))
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, startTime.Add(rotationInterval), rotator.nextRotate)

	secondItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime.Add(rotationInterval*2),
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	require.True(t, rotator.ShouldRotate(&secondItem))
	require.NoError(t, rotator.UpdateState(&secondItem))
	require.Equal(t, startTime.Add(rotationInterval*3), rotator.nextRotate)
}
