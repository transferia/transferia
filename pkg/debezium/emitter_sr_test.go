package debezium

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

func checkFoo(t *testing.T, additionalParams map[string]string) {
	params := map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	}
	for k, v := range additionalParams {
		params[k] = v
	}
	emitter, err := NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	changeItem := getUpdateChangeItem()
	currDebeziumKV, err := emitter.EmitKV(&changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Len(t, currDebeziumKV, 1)
}

func TestSR(t *testing.T) {
	check := func(t *testing.T, _ http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "my_login", username)
		require.Equal(t, "my_pass", password)
	}

	t.Run("key", func(t *testing.T) {
		callsNum := 0
		handler := func(w http.ResponseWriter, r *http.Request) {
			check(t, w, r)
			require.Equal(t, "/subjects/my_topic.my_schema_name.my_table_name-key/versions", r.RequestURI)
			callsNum++
		}
		sr := confluentsrmock.NewConfluentSRMock(nil, handler)
		defer sr.Close()
		params := map[string]string{
			debeziumparameters.KeyConverter:                  debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.KeyConverterSchemaRegistryURL: sr.URL(),
			debeziumparameters.KeyConverterBasicAuthUserInfo: "my_login:my_pass",
		}
		require.Equal(t, sr.URL(), debeziumparameters.GetKeyConverterSchemaRegistryURL(params))
		require.Equal(t, "my_login:my_pass", debeziumparameters.GetKeyConverterSchemaRegistryUserPassword(params))
		checkFoo(t, params)
		require.Equal(t, 1, callsNum)
	})

	t.Run("value", func(t *testing.T) {
		callsNum := 0
		handler := func(w http.ResponseWriter, r *http.Request) {
			check(t, w, r)
			require.Equal(t, "/subjects/my_topic.my_schema_name.my_table_name-value/versions", r.RequestURI)
			callsNum++
		}
		sr := confluentsrmock.NewConfluentSRMock(nil, handler)
		defer sr.Close()
		params := map[string]string{
			debeziumparameters.ValueConverter:                  debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.ValueConverterSchemaRegistryURL: sr.URL(),
			debeziumparameters.ValueConverterBasicAuthUserInfo: "my_login:my_pass",
		}
		require.Equal(t, sr.URL(), debeziumparameters.GetValueConverterSchemaRegistryURL(params))
		require.Equal(t, "my_login:my_pass", debeziumparameters.GetValueConverterSchemaRegistryUserPassword(params))
		checkFoo(t, params)
		require.Equal(t, 1, callsNum)
	})

	t.Run("kv", func(t *testing.T) {
		callsNum := 0
		handler := func(w http.ResponseWriter, r *http.Request) {
			check(t, w, r)
			require.True(t, r.RequestURI == "/subjects/my_topic.my_schema_name.my_table_name-key/versions" || r.RequestURI == "/subjects/my_topic.my_schema_name.my_table_name-value/versions")
			callsNum++
		}
		sr := confluentsrmock.NewConfluentSRMock(nil, handler)
		defer sr.Close()
		params := map[string]string{
			debeziumparameters.KeyConverter:                    debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.KeyConverterSchemaRegistryURL:   sr.URL(),
			debeziumparameters.KeyConverterBasicAuthUserInfo:   "my_login:my_pass",
			debeziumparameters.ValueConverter:                  debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.ValueConverterSchemaRegistryURL: sr.URL(),
			debeziumparameters.ValueConverterBasicAuthUserInfo: "my_login:my_pass",
		}
		checkFoo(t, params)
		require.Equal(t, 2, callsNum)
	})
}
