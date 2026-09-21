package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_debezium "github.com/transferia/transferia/pkg/parsers/registry/debezium"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/kafka"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	PgSource = &provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      testenv.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	YtDestination = provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/pg2lb2yt_e2e_replication",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	PgSource.WithDefaults()
	YtDestination.WithDefaults()
}

func TestReplication(t *testing.T) {
	topicName := "topic1"
	brokers := os.Getenv("KAFKA_RECIPE_BROKER_LIST")

	//------------------------------------------------------------------------------
	// init pg

	srcConnConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, PgSource)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := provider_postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	createQuery := "create table IF NOT EXISTS __test (a_id integer primary key, a_name varchar(255));"
	_, err = srcConn.Exec(context.Background(), createQuery)
	require.NoError(t, err)

	//------------------------------------------------------------------------------
	// run transfer pg -> kafka

	kafkaDst := &provider_kafka.KafkaDestination{
		Connection: &provider_kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{brokers},
		},
		Auth:  &provider_kafka.KafkaAuth{Enabled: false},
		Topic: topicName,
		FormatSettings: model.SerializationFormat{
			Name: model.SerializationFormatAuto,
		},
	}
	kafkaDst.WithDefaults()

	transfer1 := transferhelpers.MakeTransfer("test_id_pg2kafka", PgSource, kafkaDst, abstract.TransferTypeIncrementOnly)
	localWorker1 := delivery.Activate(t, transfer1)
	defer localWorker1.Close(t)

	//------------------------------------------------------------------------------
	// run transfer kafka -> yt

	parserConfigStruct := &parser_debezium.ParserConfigDebeziumCommon{}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	kafkaSrc := &provider_kafka.KafkaSource{
		Connection: &provider_kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{brokers},
		},
		Auth:             &provider_kafka.KafkaAuth{Enabled: false},
		Topic:            topicName,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
	}
	kafkaSrc.WithDefaults()

	transfer2 := transferhelpers.MakeTransfer("test_id_kafka2yt", kafkaSrc, YtDestination, abstract.TransferTypeIncrementOnly)
	localWorker2 := delivery.Activate(t, transfer2)
	defer localWorker2.Close(t)

	//------------------------------------------------------------------------------
	// replicate data

	_, err = srcConn.Exec(context.Background(), "INSERT INTO public.__test (a_id, a_name) VALUES (1, 'val1'),(2, 'val2'),(3, 'val3');")
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), "DELETE FROM public.__test WHERE a_id=1;")
	require.NoError(t, err)

	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "__test", storagecomparison.GetSampleableStorageByModel(t, YtDestination.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, storagecomparison.CompareStorages(t, PgSource, YtDestination.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
