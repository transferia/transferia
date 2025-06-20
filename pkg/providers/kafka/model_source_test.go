//go:build !disable_kafka_provider

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultKafkaSourceModel(t *testing.T) {
	model := new(KafkaSource)
	model.WithDefaults()
	require.NoError(t, model.Validate())
}
