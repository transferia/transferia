package logbroker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestWithDefaults(t *testing.T) {
	t.Run("Disable batching if nil", func(t *testing.T) {
		dst := LbDestination{
			FormatSettings: model.SerializationFormat{
				BatchingSettings: nil,
			},
		}
		dst.WithDefaults()
		require.Equal(t, &model.Batching{Enabled: false}, dst.FormatSettings.BatchingSettings)
	})

	t.Run("Leave batching as is if not nil", func(t *testing.T) {
		dst := LbDestination{
			FormatSettings: model.SerializationFormat{
				BatchingSettings: &model.Batching{Enabled: false},
			},
		}
		dst.WithDefaults()
		require.NotEmpty(t, dst.FormatSettings.Name)
		require.Equal(t, &model.Batching{Enabled: false}, dst.FormatSettings.BatchingSettings)
		require.NotNil(t, dst.FormatSettings.Settings)

		dst.FormatSettings.BatchingSettings = &model.Batching{
			Enabled:        true,
			Interval:       67,
			MaxChangeItems: 67,
			MaxMessageSize: 67,
		}
		dst.WithDefaults()
		require.NotEmpty(t, dst.FormatSettings.Name)
		require.Equal(t, &model.Batching{
			Enabled:        true,
			Interval:       67,
			MaxChangeItems: 67,
			MaxMessageSize: 67,
		}, dst.FormatSettings.BatchingSettings)
		require.NotNil(t, dst.FormatSettings.Settings)
	})

	t.Run("Codec", func(t *testing.T) {
		dst := LbDestination{}
		dst.WithDefaults()

		require.Equal(t, CompressionCodecGzip, dst.CompressionCodec)
	})
}
