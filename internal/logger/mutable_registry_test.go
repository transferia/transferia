package logger

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/util/size"
)

func TestMutableRegistry(t *testing.T) {
	solomonRegistry := solomon.NewRegistry(solomon.NewRegistryOpts().AddTags(map[string]string{"some_tag": "some_value"}))
	registry := NewMutableRegistry(solomonRegistry)

	// initialize metrics and record values without tags
	counter := registry.Counter("some_counter")
	counter.Inc()
	counter.Add(10)

	histogram := registry.Histogram("some_hist", size.DefaultBuckets())
	histogram.RecordValue(1)

	// gather metrics without tags
	metrics1, err := solomonRegistry.Gather()
	require.NoError(t, err)
	json1, err := metrics1.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "{\"metrics\":[]}", string(json1))

	// set tags
	newRegistry := registry.WithTags(map[string]string{"some_other_tag": "some_other_value"})
	require.NotEqual(t, registry, newRegistry)

	// record values with tags
	counter.Inc()
	counter.Add(10)

	histogram.RecordValue(1)

	// gather metrics with tags
	metrics2, err := solomonRegistry.Gather()
	require.NoError(t, err)
	json2, err := metrics2.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "{\"metrics\":[{\"type\":\"COUNTER\",\"labels\":{\"sensor\":\"some_counter\",\"some_other_tag\":\"some_other_value\",\"some_tag\":\"some_value\"},\"value\":11},{\"type\":\"HIST\",\"labels\":{\"sensor\":\"some_hist\",\"some_other_tag\":\"some_other_value\",\"some_tag\":\"some_value\"},\"hist\":{\"bounds\":[1024,5120,10240,51200,102400,512000,1048576,5242880,10485760,52428800,104857600,524288000,1073741824],\"buckets\":[1,0,0,0,0,0,0,0,0,0,0,0,0]}}]}", string(json2))
}
