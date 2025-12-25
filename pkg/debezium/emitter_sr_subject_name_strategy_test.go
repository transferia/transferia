package debezium

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

func getParams(sr *confluentsrmock.ConfluentSRMock, isKey bool) map[string]string {
	if isKey {
		return map[string]string{
			debeziumparameters.KeyConverter:                  debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.KeyConverterSchemaRegistryURL: sr.URL(),
		}
	} else {
		return map[string]string{
			debeziumparameters.ValueConverter:                  debeziumparameters.ConverterConfluentJSON,
			debeziumparameters.ValueConverterSchemaRegistryURL: sr.URL(),
		}
	}
}

func getTopicSettings(in map[string]string, isFullTopicName bool) map[string]string {
	if isFullTopicName {
		in[debeziumparameters.TopicPrefix] = "topic_full_path"
	} else {
		in[debeziumparameters.TopicPrefix] = "topic_prefix"
	}
	return in
}

func getStrategy(in map[string]string, isKey bool, strategy string) map[string]string {
	if isKey {
		in[debeziumparameters.KeySubjectNameStrategy] = strategy
	} else {
		in[debeziumparameters.ValueSubjectNameStrategy] = strategy
	}
	return in
}

func check(t *testing.T, isKey, isFullTopic bool, nameStrategy, expectedName string) {
	counter := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		counter++
		tmp := r.RequestURI
		tmp = strings.TrimPrefix(tmp, "/subjects/")
		tmp = strings.TrimSuffix(tmp, "/versions")
		require.Equal(t, expectedName, tmp)
	}
	sr := confluentsrmock.NewConfluentSRMock(nil, handler)
	defer sr.Close()
	checkFoo(t, getStrategy(getTopicSettings(getParams(sr, isKey), isFullTopic), isKey, nameStrategy))
	require.Equal(t, 1, counter)
}

func TestSubjectNameStrategy(t *testing.T) {
	// key

	t.Run("key:TopicNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, true, true, debeziumparameters.SubjectTopicNameStrategy, "topic_full_path.my_schema_name.my_table_name-key")
	})
	t.Run("key:RecordNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, true, true, debeziumparameters.SubjectRecordNameStrategy, "topic_full_path.my_schema_name.my_table_name.Key")
	})
	t.Run("key:TopicRecordNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, true, true, debeziumparameters.SubjectTopicRecordNameStrategy, "topic_full_path.my_schema_name.my_table_name-topic_full_path.my_schema_name.my_table_name.Key")
	})

	t.Run("key:SubjectTopicNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, true, false, debeziumparameters.SubjectTopicNameStrategy, "topic_prefix.my_schema_name.my_table_name-key")
	})
	t.Run("key:SubjectRecordNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, true, false, debeziumparameters.SubjectRecordNameStrategy, "topic_prefix.my_schema_name.my_table_name.Key")
	})
	t.Run("key:TopicRecordNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, true, false, debeziumparameters.SubjectTopicRecordNameStrategy, "topic_prefix.my_schema_name.my_table_name-topic_prefix.my_schema_name.my_table_name.Key")
	})

	// val

	t.Run("val:TopicNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, false, true, debeziumparameters.SubjectTopicNameStrategy, "topic_full_path.my_schema_name.my_table_name-value")
	})
	t.Run("val:RecordNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, false, true, debeziumparameters.SubjectRecordNameStrategy, "topic_full_path.my_schema_name.my_table_name.Envelope")
	})
	t.Run("val:TopicRecordNameStrategy:full-topic-name", func(t *testing.T) {
		check(t, false, true, debeziumparameters.SubjectTopicRecordNameStrategy, "topic_full_path.my_schema_name.my_table_name-topic_full_path.my_schema_name.my_table_name.Envelope")
	})

	t.Run("val:SubjectTopicNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, false, false, debeziumparameters.SubjectTopicNameStrategy, "topic_prefix.my_schema_name.my_table_name-value")
	})
	t.Run("val:SubjectRecordNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, false, false, debeziumparameters.SubjectRecordNameStrategy, "topic_prefix.my_schema_name.my_table_name.Envelope")
	})
	t.Run("val:TopicRecordNameStrategy:topic-prefix", func(t *testing.T) {
		check(t, false, false, debeziumparameters.SubjectTopicRecordNameStrategy, "topic_prefix.my_schema_name.my_table_name-topic_prefix.my_schema_name.my_table_name.Envelope")
	})
}
