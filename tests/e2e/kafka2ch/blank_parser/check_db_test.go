package blankparser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	parser_blank "github.com/transferia/transferia/pkg/parsers/registry/blank"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	kafka_client "github.com/transferia/transferia/pkg/providers/kafka/client"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/transformer"
	transformer_jsonparser "github.com/transferia/transferia/pkg/transformer/registry/jsonparser"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/kafka"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestLogs(t *testing.T) {
	src, err := provider_kafka.SourceRecipe()
	require.NoError(t, err)
	src.Topic = "logs"
	kafkaClient, err := kafka_client.NewClient(src.Connection.Brokers, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, src.Topic, nil))
	dst, err := chrecipe.Target(chrecipe.WithInitFile("ch_init.sql"), chrecipe.WithDatabase("mtmobproxy"))
	require.NoError(t, err)

	src.Topic = "logs"
	parserConfigMap, err := parsers.ParserConfigStructToMap(new(parser_blank.ParserConfigBlankLb))
	require.NoError(t, err)
	src.ParserConfig = parserConfigMap
	require.NoError(t, err)
	transfer := &model.Transfer{
		ID:  "e2e_test",
		Src: src,
		Dst: dst,
	}
	transfer.Transformation = &model.Transformation{
		Transformers: &transformer.Transformers{Transformers: []transformer.Transformer{{
			transformer_jsonparser.TransformerType: &transformer_jsonparser.Config{
				Parser: &parser_json.ParserConfigJSONCommon{
					Fields: []abstract.ColSchema{
						{ColumnName: "msg", DataType: ytschema.TypeString.String()},
					},
					AddRest:       false,
					AddDedupeKeys: true,
				},
				Topic: "logs",
			},
		}}},
	}

	lgr, closer, err := logger.NewKafkaLogger(&logger.KafkaConfig{
		Broker:   src.Connection.Brokers[0],
		Topic:    src.Topic,
		User:     src.Auth.User,
		Password: src.Auth.Password,
	})
	require.NoError(t, err)

	defer closer.Close()
	// SEND TO KAFKA
	go func() {
		for i := 0; i < 50; i++ {
			lgr.Infof("line:%v", i)
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	w.Start()
	require.NoError(t, storage.WaitDestinationEqualRowsCount(dst.Database, src.Topic, storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second, 50))
	require.NoError(t, w.Stop())
}
