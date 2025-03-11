package confluent

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
)

func TestSetCredentials(t *testing.T) {
	t.Run("on-premise Schema Registry", func(t *testing.T) {
		client, err := NewSchemaRegistryClientWithTransport("https://my-lovely-host.yandex-team.ru:443", "", logger.Log)
		require.NoError(t, err)
		client.SetCredentials("my-user-name", "my-password")
		require.Equal(t, "my-user-name", client.credentials.username)
	})

	t.Run("Yandex Schema Registry (prod)", func(t *testing.T) {
		client, err := NewSchemaRegistryClientWithTransport("https://srncq37em9mhuos49f33.schema-registry.yandex-team.ru", "", logger.Log)
		require.NoError(t, err)
		client.SetCredentials("my-user-name", "my-password")
		require.Equal(t, "OAuth", client.credentials.username)
	})

	t.Run("Yandex Schema Registry (preprod)", func(t *testing.T) {
		client, err := NewSchemaRegistryClientWithTransport("https://srncq37em9mhuos49f33.schema-registry-preprod.yandex-team.ru", "", logger.Log)
		require.NoError(t, err)
		client.SetCredentials("my-user-name", "my-password")
		require.Equal(t, "OAuth", client.credentials.username)
	})
}
