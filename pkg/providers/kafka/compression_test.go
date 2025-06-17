//go:build !disable_kafka_provider

package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestReadWriteWithCompression(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)
	dst, err := DestinationRecipe()
	require.NoError(t, err)
	dst.FormatSettings.Name = model.SerializationFormatMirror
	tc := func(compression Encoding) {
		kafkaSource.Topic = "topic_" + string(compression)
		dst.Topic = "topic_" + string(compression)
		dst.Compression = compression
		currSink, err := NewReplicationSink(dst, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
		require.NoError(t, err)
		require.NoError(t, currSink.Push([]abstract.ChangeItem{*sinkTestMirrorChangeItem}))
		time.Sleep(time.Second) // just in case

		src, err := NewSource("asd", kafkaSource, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		items, err := src.Fetch()
		require.NoError(t, err)
		src.Stop()
		require.Len(t, items, 1)
		require.Len(t, items[0].ColumnValues, 5)
		require.Equal(t, items[0].ColumnValues[4], "blablabla")
		abstract.Dump(items)
	}
	t.Run("gzip", func(t *testing.T) {
		tc(GzipEncoding)
	})
	t.Run("snappy", func(t *testing.T) {
		tc(SnappyEncoding)
	})
	t.Run("lz4", func(t *testing.T) {
		tc(LZ4Encoding)
	})
	t.Run("zstd", func(t *testing.T) {
		tc(ZstdEncoding)
	})
}
