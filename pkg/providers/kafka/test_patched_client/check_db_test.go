package replication

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	server "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func buildSink(t *testing.T, broker, topicName, topicPrefix string, encoding kafka.Encoding, serializationFormat server.SerializationFormatName) abstract.Sinker {
	kafkaDst := &kafka.KafkaDestination{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     server.DisabledTLS,
			Brokers: []string{broker},
		},
		Auth:        &kafka.KafkaAuth{Enabled: false},
		Topic:       topicName,
		TopicPrefix: topicPrefix,
		FormatSettings: server.SerializationFormat{
			Name: serializationFormat,
		},
		Compression: encoding,
	}
	kafkaDst.WithDefaults()
	sink, err := kafka.NewReplicationSink(
		kafkaDst,
		solomon.NewRegistry(nil),
		logger.Log,
	)
	require.NoError(t, err)
	return sink
}

func makeChangeItem(topicName, val string) abstract.ChangeItem {
	return abstract.MakeRawMessage(
		[]byte("_"),
		"table",
		time.Time{},
		topicName,
		1, // shard
		0, // offset
		[]byte(val),
	)
}

func createTopic(t *testing.T, broker, topicName string) {
	sinkWithoutCompression := buildSink(t, broker, topicName, "", kafka.NoEncoding, server.SerializationFormatJSON)
	require.NoError(t, sinkWithoutCompression.Push([]abstract.ChangeItem{makeChangeItem(topicName, "blablabla")}))
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func setMaxMessageBytes(t *testing.T, kafkaClient *client.Client, topicName, value string) {
	// set max.message.bytes
	err := kafkaClient.AlterConfigs(topicName, "max.message.bytes", value)
	require.NoError(t, err)
	// check max.message.bytes
	newMaxMessageBytes, err := kafkaClient.DescribeConfigs(topicName, "max.message.bytes")
	require.NoError(t, err)
	require.Equal(t, value, strconv.Itoa(int(newMaxMessageBytes)))
}

func TestAutoDeriveBatchBytes(t *testing.T) {
	broker := os.Getenv("KAFKA_RECIPE_BROKER_LIST")
	topicName := "topic1"

	// create topics
	createTopic(t, broker, topicName)

	kafkaClient, err := client.NewClient([]string{broker}, nil, nil, nil)
	require.NoError(t, err)

	setMaxMessageBytes(t, kafkaClient, topicName, "2000000")

	sinkWithoutCompression := buildSink(t, broker, topicName, "", kafka.NoEncoding, server.SerializationFormatJSON)
	sinkWithCompression := buildSink(t, broker, topicName, "", kafka.LZ4Encoding, server.SerializationFormatJSON)

	// try to push 1.5mb - expect OK
	require.NoError(t, sinkWithoutCompression.Push([]abstract.ChangeItem{makeChangeItem(topicName, strings.Repeat("x", 3*512*1024))}))
	// try to push 2.5mb without compression - expects ERROR
	require.Error(t, sinkWithoutCompression.Push([]abstract.ChangeItem{makeChangeItem(topicName, strings.Repeat("x", 5*512*1024))}))
	// try to push 2.5mb with compression LOW_ENTROPY - expects OK
	require.NoError(t, sinkWithCompression.Push([]abstract.ChangeItem{makeChangeItem(topicName, strings.Repeat("x", 5*512*1024))}))
	// try to push 2.5mb with compression HIGH_ENTROPY - expects ERROR
	require.Error(t, sinkWithCompression.Push([]abstract.ChangeItem{makeChangeItem(topicName, randomString(5*512*1024))}))

	//-----------------------------------------------------------------------------------------------------------------
	// check topicPrefix - writing into 2 topics via one writer

	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "k", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:integer", PrimaryKey: true},
		{ColumnName: "v", DataType: ytschema.TypeString.String(), OriginalType: "pg:text", PrimaryKey: false},
	})
	table1ItemsBuilder := helpers.NewChangeItemsBuilder("public", "table1", tableSchema)
	table2ItemsBuilder := helpers.NewChangeItemsBuilder("public", "table2", tableSchema)

	insertIntoTable1 := table1ItemsBuilder.Inserts(t, []map[string]interface{}{{"k": 1, "v": strings.Repeat("x", 5*512*1024)}})
	insertIntoTable2 := table2ItemsBuilder.Inserts(t, []map[string]interface{}{{"k": 1, "v": strings.Repeat("x", 5*512*1024)}})

	prefix := "my_topic_prefix"
	topicPrefixed1 := prefix + ".public.table1"
	topicPrefixed2 := prefix + ".public.table2"

	createTopic(t, broker, topicPrefixed1)
	createTopic(t, broker, topicPrefixed2)

	setMaxMessageBytes(t, kafkaClient, topicPrefixed1, "10000000")
	setMaxMessageBytes(t, kafkaClient, topicPrefixed2, "100000")

	sinkWithoutCompressionPrefixed := buildSink(t, broker, "", prefix, kafka.NoEncoding, server.SerializationFormatDebezium)
	// try to push 1.5mb to my_topic_prefix.public.table1 - expect OK
	require.NoError(t, sinkWithoutCompressionPrefixed.Push(insertIntoTable1))
	// try to push 1.5mb to my_topic_prefix.public.table1 - expect ERROR
	require.Error(t, sinkWithoutCompressionPrefixed.Push(insertIntoTable2))
}
