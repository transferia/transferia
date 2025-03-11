package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/debezium"
	"github.com/transferria/transferria/pkg/providers/kafka"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/yt"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	PgSource = &pgcommon.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	YtDestination = yt.NewYtDestinationV1(yt.YtDestination{
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

	srcConnConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, PgSource)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := pgcommon.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	createQuery := "create table IF NOT EXISTS __test (a_id integer primary key, a_name varchar(255));"
	_, err = srcConn.Exec(context.Background(), createQuery)
	require.NoError(t, err)

	//------------------------------------------------------------------------------
	// run transfer pg -> kafka

	kafkaDst := &kafka.KafkaDestination{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{brokers},
		},
		Auth:  &kafka.KafkaAuth{Enabled: false},
		Topic: topicName,
		FormatSettings: model.SerializationFormat{
			Name: model.SerializationFormatAuto,
		},
	}
	kafkaDst.WithDefaults()

	transfer1 := helpers.MakeTransfer("test_id_pg2kafka", PgSource, kafkaDst, abstract.TransferTypeIncrementOnly)
	localWorker1 := helpers.Activate(t, transfer1)
	defer localWorker1.Close(t)

	//------------------------------------------------------------------------------
	// run transfer kafka -> yt

	parserConfigStruct := &debezium.ParserConfigDebeziumCommon{}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	kafkaSrc := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{brokers},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            topicName,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
		IsHomo:           false,
	}
	kafkaSrc.WithDefaults()

	transfer2 := helpers.MakeTransfer("test_id_kafka2yt", kafkaSrc, YtDestination, abstract.TransferTypeIncrementOnly)
	localWorker2 := helpers.Activate(t, transfer2)
	defer localWorker2.Close(t)

	//------------------------------------------------------------------------------
	// replicate data

	_, err = srcConn.Exec(context.Background(), "INSERT INTO public.__test (a_id, a_name) VALUES (1, 'val1'),(2, 'val2'),(3, 'val3');")
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), "DELETE FROM public.__test WHERE a_id=1;")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test", helpers.GetSampleableStorageByModel(t, YtDestination.LegacyModel()), 60*time.Second, 2))
	require.NoError(t, helpers.CompareStorages(t, PgSource, YtDestination.LegacyModel(), helpers.NewCompareStorageParams()))
}
