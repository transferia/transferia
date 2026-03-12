package topicsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	lbrecipe "github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/parsers/registry/blank"
	"github.com/transferia/transferia/pkg/providers/ydb"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"github.com/transferia/transferia/tests/helpers/lbenv"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

var (
	sinkTestTypicalChangeItem  *abstract.ChangeItem
	sinkTestLbMirrorChangeItem *abstract.ChangeItem
)

func init() {
	testChangeItem := `{"id":601,"nextlsn":25051056,"commitTime":1643660670333075000,"txPosition":0,"kind":"insert","schema":"public","table":"basic_types15","columnnames":["id","val"],"columnvalues":[1,-8388605],"table_schema":[{"path":"","name":"id","type":"int32","key":true,"required":false,"original_type":"pg:integer","original_type_params":null},{"path":"","name":"val","type":"int32","key":false,"required":false,"original_type":"pg:integer","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestTypicalChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testChangeItem))

	// mirrorChangeItem
	// in format of pkg/parsers/blank_parser (if format from logfeller/source.go)
	testMirrorChangeItem := `{"id":0,"nextlsn":123,"commitTime":234,"txPosition":0,"kind":"insert","schema":"raw_data","table":"my_topic_name","columnnames":["partition","offset","seq_no","source_id","c_time","w_time","ip","lb_raw_message","lb_extra_fields"],"columnvalues":["{\"cluster\":\"\",\"partition\":345,\"topic\":\"\"}",456,567,"my_source_id","2022-03-23T19:30:51.911+03:00","2022-03-24T19:30:51.911+03:00","1.1.1.1","my_data",{"my_key1":"my_val1","my_key2":"my_val2"}],"table_schema":[{"path":"","name":"partition","type":"string","key":true,"required":false,"original_type":""},{"path":"","name":"offset","type":"uint64","key":true,"required":false,"original_type":""},{"path":"","name":"seq_no","type":"uint64","key":false,"required":false,"original_type":""},{"path":"","name":"source_id","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"c_time","type":"datetime","key":false,"required":false,"original_type":""},{"path":"","name":"w_time","type":"datetime","key":false,"required":false,"original_type":""},{"path":"","name":"ip","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"lb_raw_message","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"lb_extra_fields","type":"any","key":false,"required":false,"original_type":""}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestLbMirrorChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testMirrorChangeItem))
	rawDataIndex := blank.BlankColsIDX[blank.RawMessageColumn]
	rawData := sinkTestLbMirrorChangeItem.ColumnValues[rawDataIndex]
	sinkTestLbMirrorChangeItem.ColumnValues[rawDataIndex] = []byte(rawData.(string))
	extrasIndex := blank.BlankColsIDX[blank.ExtrasColumn]
	extrasMapToInterface := sinkTestLbMirrorChangeItem.ColumnValues[extrasIndex]
	extrasStr, _ := json.Marshal(extrasMapToInterface)
	var extras map[string]string
	_ = json.Unmarshal(extrasStr, &extras)
	sinkTestLbMirrorChangeItem.ColumnValues[extrasIndex] = extras
}

func newTestSinkWithDefaultWriterFactory(t *testing.T, dst *Config) *sink {
	result, err := newSinkWithFactories(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		"my_transfer_id",
		false)

	require.NoError(t, err)

	lbSink, ok := result.(*sink)
	require.True(t, ok)

	return lbSink
}

//---------------------------------------------------------------------------------------------------------------------

func TestFormats(t *testing.T) {
	lbEnv := lbrecipe.New(t)

	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	dstTemplate := newConfigForTest(instance, port, database, creds)

	t.Run("Test JSON", func(t *testing.T) {
		topicPath := "json_topic"
		topicFullPath := path.Join(database, topicPath)

		createTopic(t, topicPath)

		dst := dstTemplate
		dst.Topic = topicPath
		dst.FormatSettings = newFormatSettings(model.SerializationFormatJSON, nil)

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem}))
		require.NoError(t, testSink.Close())

		var toCanon []map[string]any
		handler := func(topic string, index int, msg persqueue.ReadMessage) {
			toCanon = append(toCanon, map[string]any{
				"topic": topic,
				"index": index,
				"data":  lbenv.MessageDataForCanon(t, msg),
			})
		}

		lbenv.LoadMessages(t, lbEnv, dst.Database, topicFullPath, 1, handler)
		canon.SaveJSON(t, toCanon)
	})

	t.Run("Test Debezium", func(t *testing.T) {
		topicPrefix := "debezium_topic"
		expectedTopicName := topicPrefix + ".public.basic_types15"
		expectedTopicFullPath := path.Join(database, expectedTopicName)

		createTopic(t, expectedTopicName)

		dst := dstTemplate
		dst.TopicPrefix = topicPrefix
		dst.FormatSettings = newFormatSettings(
			model.SerializationFormatDebezium,
			map[string]string{
				debeziumparameters.SourceType: "pg",
			},
		)

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem}))
		require.NoError(t, testSink.Close())

		var toCanon []map[string]any
		handler := func(topic string, index int, msg persqueue.ReadMessage) {
			toCanon = append(toCanon, map[string]any{
				"topic": topic,
				"index": index,
				"data":  lbenv.MessageDataForCanon(t, msg),
			})
		}

		lbenv.LoadMessages(t, lbEnv, dst.Database, expectedTopicFullPath, 1, handler)
		canon.SaveJSON(t, toCanon)
	})

	t.Run("Test Mirror", func(t *testing.T) {
		topicPath := "mirror_topic"
		topicFullPath := path.Join(database, topicPath)

		createTopic(t, topicPath)

		dst := dstTemplate
		dst.Topic = topicPath
		dst.FormatSettings = newFormatSettings(model.SerializationFormatLbMirror, nil)

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestLbMirrorChangeItem}))
		require.NoError(t, testSink.Close())

		var toCanon []map[string]any
		handler := func(topic string, index int, msg persqueue.ReadMessage) {
			toCanon = append(toCanon, map[string]any{
				"topic": topic,
				"index": index,
				"data":  string(msg.Data),
			})
		}

		lbenv.LoadMessages(t, lbEnv, dst.Database, topicFullPath, 1, handler)
		canon.SaveJSON(t, toCanon)
	})

	t.Run("Test Native", func(t *testing.T) {
		topicPath := "native_topic"
		topicFullPath := path.Join(database, topicPath)

		createTopic(t, topicPath)

		dst := dstTemplate
		dst.Topic = topicPath
		dst.FormatSettings = newFormatSettings(model.SerializationFormatNative, nil)

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem}))
		require.NoError(t, testSink.Close())

		dataCmp := func(topic string, index int, msg persqueue.ReadMessage) {
			jsonBytes, err := json.Marshal(sinkTestTypicalChangeItem)
			require.NoError(t, err)
			require.Equal(t, "["+string(jsonBytes)+"]", string(msg.Data))
		}

		lbenv.LoadMessages(t, lbEnv, dst.Database, topicFullPath, 1, dataCmp)
	})
}

func TestAddDTSystemTables(t *testing.T) {
	lbEnv := lbrecipe.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	topicPrefix := "system_tables_topic"
	topicPrefixPath := path.Join(database, topicPrefix)

	dstTemplate := newConfigForTest(instance, port, database, creds)
	dstTemplate.TopicPrefix = topicPrefix
	dstTemplate.FormatSettings = newFormatSettings(
		model.SerializationFormatDebezium,
		map[string]string{
			debeziumparameters.SourceType: "pg",
		},
	)

	currChangeItem := *sinkTestTypicalChangeItem
	currChangeItem.Table = abstract.TableConsumerKeeper

	t.Run("false", func(t *testing.T) {
		createTopic(t, topicPrefix)

		dst := dstTemplate
		dst.AddSystemTables = false

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{currChangeItem}))
		require.NoError(t, testSink.Close())

		lbenv.LoadMessages(t, lbEnv, database, topicPrefixPath, 0, nil)
	})

	t.Run("true", func(t *testing.T) {
		systemTopicName := fmt.Sprintf("%s.public.%s", topicPrefix, abstract.TableConsumerKeeper)
		systemTopicPath := path.Join(database, systemTopicName)
		createTopic(t, systemTopicName)

		dst := dstTemplate
		dst.AddSystemTables = true

		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{currChangeItem}))
		require.NoError(t, testSink.Close())

		var toCanon []map[string]any
		handler := func(topic string, index int, msg persqueue.ReadMessage) {
			toCanon = append(toCanon, map[string]any{
				"topic": topic,
				"index": index,
				"data":  lbenv.MessageDataForCanon(t, msg),
			})
		}

		lbenv.LoadMessages(t, lbEnv, database, systemTopicPath, 1, handler)
		canon.SaveJSON(t, toCanon)
	})
}

// TM-4075
// @nyoroon case, sharded replication lb(YQL-script)->lb with turned on setting: 'Split into sub tables'
// every table (sub table) should be written simultaneously (with own sourceID)
func TestSimultaneouslyWriteTables(t *testing.T) {
	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	topicName := "simultaneously_write_topic"
	topicPath := path.Join(database, topicName)
	createTopic(t, topicName)

	dst := newConfigForTest(instance, port, database, creds)
	dst.Topic = topicName
	dst.FormatSettings = newFormatSettings(model.SerializationFormatJSON, nil)

	testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
	require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem}))
	require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem}))
	require.NoError(t, testSink.Close())

	lbenv.LoadMessages(t, lbrecipe.New(t), dst.Database, topicPath, 2, nil)
}

func TestResetWriters(t *testing.T) {
	topicName := "reset_writers_topic"
	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)

	dst := newConfigForTest(instance, port, database, creds)
	dst.Topic = topicName
	dst.FormatSettings = newFormatSettings(model.SerializationFormatNative, nil)

	testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
	require.NoError(t, testSink.Push([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()}))
	require.NoError(t, testSink.Close())

	require.Equal(t, 0, testSink.writers.Len())
}

func TestSynchronizeEvents(t *testing.T) {
	lbEnv := lbrecipe.New(t)

	instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	dstTemplate := newConfigForTest(instance, port, database, creds)
	dstTemplate.FormatSettings = newFormatSettings(model.SerializationFormatNative, nil)

	// test just one synchronize event - nothing should be written into the logbroker
	t.Run("Without data", func(t *testing.T) {
		topicPath := "synch_no_data_topic" // this topic wasn't created, so there will be an error, if sink tries to write msg

		dst := dstTemplate
		dst.Topic = topicPath
		testSink, err := newSinkWithFactories(
			&dst,
			solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
			logger.Log,
			"my_transfer_id",
			false,
		)
		require.NoError(t, err)

		require.NoError(t, testSink.Push([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()}))
		require.NoError(t, testSink.Close())
	})

	// test one data event + one synchronize event - only data event should be written into the logbroker
	t.Run("With data", func(t *testing.T) {
		topicPath := "synch_with_data_topic"
		topicFullPath := path.Join(database, topicPath)

		createTopic(t, topicPath)

		dst := dstTemplate
		dst.Topic = topicPath
		testSink := newTestSinkWithDefaultWriterFactory(t, &dst)
		require.NoError(t, testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem, abstract.MakeSynchronizeEvent()}))
		require.NoError(t, testSink.Close())

		dataCmp := func(topic string, index int, msg persqueue.ReadMessage) {
			jsonBytes, _ := json.Marshal(sinkTestTypicalChangeItem)
			require.Equal(t, "["+string(jsonBytes)+"]", string(msg.Data))
		}

		lbenv.LoadMessages(t, lbEnv, dst.Database, topicFullPath, 1, dataCmp)

	})
}

func TestSplitSerializedMessages(t *testing.T) {
	compareData := func(t *testing.T, testMessages []serializer.SerializedMessage, resMessages [][]topicwriter.Message) {
		resValues := make([][]byte, 0)
		for _, batch := range resMessages {
			for _, message := range batch {
				buf := bytes.Buffer{}
				_, err := buf.ReadFrom(message.Data)
				require.NoError(t, err)

				resValues = append(resValues, buf.Bytes())
			}
		}

		for idx, testMessage := range testMessages {
			require.Equal(t, testMessage.Value, resValues[idx])
		}
	}

	t.Run("NoMessagesTest", func(t *testing.T) {
		t.Parallel()
		msgsSize, msgBatches := splitSerializedMessages(10, []serializer.SerializedMessage{})
		require.Equal(t, 0, msgsSize)
		require.Empty(t, msgBatches)
	})

	t.Run("CommonTest", func(t *testing.T) {
		t.Parallel()
		testMessages := []serializer.SerializedMessage{
			{Value: []byte("{}")},
			{Value: []byte("{\"some_data\": \"data\"}")},
			{Value: []byte("{\"data\": \"other_data\"}")},
		}

		msgsSize, msgBatches := splitSerializedMessages(10, testMessages)
		require.Equal(t, 45, msgsSize)

		require.Len(t, msgBatches, 2)
		require.Len(t, msgBatches[0], 2)
		require.Len(t, msgBatches[1], 1)
		compareData(t, testMessages, msgBatches)
	})

	t.Run("MaxSizeLessThanMessageSizeTest", func(t *testing.T) {
		t.Parallel()
		testMessages := []serializer.SerializedMessage{
			{Value: []byte("{\"some_data\": \"data\"}")},
			{Value: []byte("{\"data\": \"other_data\"}")},
		}

		msgsSize, msgBatches := splitSerializedMessages(5, testMessages)
		require.Equal(t, 43, msgsSize)

		require.Len(t, msgBatches, 2)
		require.Len(t, msgBatches[0], 1)
		require.Len(t, msgBatches[1], 1)
		compareData(t, testMessages, msgBatches)
	})

	t.Run("SerializedMessagesSatisfyConstraintTest", func(t *testing.T) {
		t.Parallel()
		testMessages := []serializer.SerializedMessage{
			{Value: []byte("{}")},
			{Value: []byte("{\"some_data\": \"data\"}")},
			{Value: []byte("{\"data\": \"other_data\"}")},
		}

		msgsSize, msgBatches := splitSerializedMessages(1000, testMessages)
		require.Equal(t, 45, msgsSize)

		require.Len(t, msgBatches, 1)
		require.Len(t, msgBatches[0], 3)
		compareData(t, testMessages, msgBatches)
	})
}

func newConfigForTest(instance string, port int, database string, creds ydb.TokenCredentials) Config {
	return Config{
		Endpoint:         fmt.Sprintf("%s:%d", instance, port),
		Database:         database,
		Credentials:      creds,
		TLS:              model.DisabledTLS,
		CompressionCodec: CompressionCodecGzip,
		FormatSettings:   newFormatSettings(model.SerializationFormatAuto, nil),
	}
}

func newFormatSettings(name model.SerializationFormatName, settings map[string]string) model.SerializationFormat {
	if settings == nil {
		settings = make(map[string]string)
	}
	return model.SerializationFormat{
		Name:     name,
		Settings: settings,
		BatchingSettings: &model.Batching{
			Enabled:        false,
			Interval:       0,
			MaxChangeItems: 0,
			MaxMessageSize: 0,
		},
	}
}

func createTopic(t *testing.T, topicName string) {
	defaultConsumer := "test_client"
	require.NoError(t, ydbrecipe.Driver(t).Topic().Create(context.Background(), topicName, topicoptions.CreateWithConsumer(topictypes.Consumer{
		Name:            defaultConsumer,
		SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
	})))
}
