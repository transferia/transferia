package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/debezium"
	"github.com/transferia/transferia/pkg/providers/kafka"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2kafka2yt_e2e_alters")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestSnapshotAndIncrement(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "YT target", Port: targetPort},
	))

	//-----------------------------------------------------------------------------------------------------------------
	// pg -> kafka

	topic := "dbserver1"

	httpPort := os.Getenv("SR_HTTP_PORT")
	schemaRegistryURL := fmt.Sprintf("http://localhost:%s", httpPort)

	dst, err := kafka.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = topic
	dst.FormatSettings = model.SerializationFormat{
		Name: model.SerializationFormatDebezium,
		Settings: map[string]string{
			"value.converter":                                        "io.confluent.connect.json.JsonSchemaConverter",
			"value.converter.schema.registry.url":                    schemaRegistryURL,
			"value.converter.basic.auth.user.info":                   "Oauth:blablabla",
			"value.converter.basic.auth.credentials.source":          "USER_INFO",
			"value.converter.dt.json.generate.closed.content.schema": "true",
			"dt.add.original.type.info":                              "true",
		},
	}
	dst.WithDefaults()

	helpers.InitSrcDst(helpers.TransferID, &Source, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer1 := &model.Transfer{
		ID:   "test_id_pg2kafka",
		Src:  &Source,
		Dst:  dst,
		Type: abstract.TransferTypeSnapshotAndIncrement,
	}

	worker1 := helpers.Activate(t, transfer1)
	defer worker1.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// kafka -> pg

	parserConfigStruct := &debezium.ParserConfigDebeziumCommon{
		SchemaRegistryURL: schemaRegistryURL,
		SkipAuth:          true,
		Username:          "",
		Password:          "",
		TLSFile:           "",
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            topic,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
		IsHomo:           false,
	}
	src.WithDefaults()

	helpers.InitSrcDst(helpers.TransferID, src, Target, abstract.TransferTypeIncrementOnly)
	transfer2 := &model.Transfer{
		ID:   "test_id_kafka2yt",
		Src:  src,
		Dst:  Target,
		Type: abstract.TransferTypeIncrementOnly,
	}

	worker2 := helpers.Activate(t, transfer2)
	defer worker2.Close(t)

	//-----------------------------------------------------------------------------------------------------------------

	time.Sleep(time.Second * 10)

	//---

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), "ALTER TABLE public.basic_types ADD COLUMN v2 TEXT;")
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), "INSERT INTO public.basic_types (k, v1, v2) VALUES (2, 'a', 'b');")
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "basic_types", helpers.GetSampleableStorageByModel(t, Target), 180*time.Second, 2))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
