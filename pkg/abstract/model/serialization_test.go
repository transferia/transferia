package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func TestSanitizeSecrets(t *testing.T) {
	tests := []struct {
		name       string
		settingsKV [][2]string
		expectedKV [][2]string
	}{
		{
			name: "sensitive parameters are sanitized",
			settingsKV: [][2]string{
				{debeziumparameters.KeyConverterBasicAuthUserInfo, "secret_key_password"},
				{debeziumparameters.ValueConverterBasicAuthUserInfo, "secret_value_password"},
				{debeziumparameters.DatabaseDBName, "test_db"},
				{debeziumparameters.TopicPrefix, "test_topic"},
			},
			expectedKV: [][2]string{
				{debeziumparameters.KeyConverterBasicAuthUserInfo, "***SENSITIVE***"},
				{debeziumparameters.ValueConverterBasicAuthUserInfo, "***SENSITIVE***"},
				{debeziumparameters.DatabaseDBName, "test_db"},
				{debeziumparameters.TopicPrefix, "test_topic"},
			},
		},
		{
			name: "no sensitive parameters",
			settingsKV: [][2]string{
				{debeziumparameters.DatabaseDBName, "test_db"},
				{debeziumparameters.TopicPrefix, "test_topic"},
				{debeziumparameters.UnknownTypesPolicy, "fail"},
			},
			expectedKV: [][2]string{
				{debeziumparameters.DatabaseDBName, "test_db"},
				{debeziumparameters.TopicPrefix, "test_topic"},
				{debeziumparameters.UnknownTypesPolicy, "fail"},
			},
		},
		{
			name:       "empty settings",
			settingsKV: [][2]string{},
			expectedKV: [][2]string{},
		},
		{
			name: "mixed sensitive and non-sensitive parameters",
			settingsKV: [][2]string{
				{debeziumparameters.KeyConverter, "org.apache.kafka.connect.json.JsonConverter"},
				{debeziumparameters.KeyConverterBasicAuthUserInfo, "user:password"},
				{debeziumparameters.ValueConverter, "org.apache.kafka.connect.json.JsonConverter"},
				{debeziumparameters.ValueConverterBasicAuthUserInfo, "another_user:another_password"},
				{debeziumparameters.KeyConverterSchemaRegistryURL, "http://schema-registry:8081"},
			},
			expectedKV: [][2]string{
				{debeziumparameters.KeyConverter, "org.apache.kafka.connect.json.JsonConverter"},
				{debeziumparameters.KeyConverterBasicAuthUserInfo, "***SENSITIVE***"},
				{debeziumparameters.ValueConverter, "org.apache.kafka.connect.json.JsonConverter"},
				{debeziumparameters.ValueConverterBasicAuthUserInfo, "***SENSITIVE***"},
				{debeziumparameters.KeyConverterSchemaRegistryURL, "http://schema-registry:8081"},
			},
		},
	}
	settingsKVtoMap := func(settingsKV [][2]string) map[string]string {
		result := make(map[string]string, len(settingsKV))
		for _, keyValue := range settingsKV {
			result[keyValue[0]] = keyValue[1]
		}
		return result
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of settingsKV to avoid modifying test data
			settingsKV := make([][2]string, len(tt.settingsKV))
			copy(settingsKV, tt.settingsKV)
			settings := settingsKVtoMap(tt.settingsKV)
			expectedSettings := settingsKVtoMap(tt.expectedKV)

			sf := &SerializationFormat{
				Name:       SerializationFormatDebezium,
				Settings:   settings,
				SettingsKV: settingsKV,
			}

			// Call SanitizeSecrets
			sf.SanitizeSecrets()

			// Verify that sensitive parameters are sanitized
			require.Equal(t, tt.expectedKV, sf.SettingsKV)
			require.Equal(t, expectedSettings, sf.Settings)
		})
	}
}
