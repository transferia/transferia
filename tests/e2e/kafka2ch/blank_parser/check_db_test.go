package blankparser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/blank"
	"github.com/transferria/transferria/pkg/parsers/registry/json"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	"github.com/transferria/transferria/pkg/providers/kafka"
	"github.com/transferria/transferria/pkg/providers/kafka/client"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/jsonparser"
	"github.com/transferria/transferria/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestLogs(t *testing.T) {
	src, err := kafka.SourceRecipe()
	require.NoError(t, err)
	src.Topic = "logs"
	kafkaClient, err := client.NewClient(src.Connection.Brokers, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, src.Topic, nil))
	dst, err := chrecipe.Target(chrecipe.WithInitFile("ch_init.sql"), chrecipe.WithDatabase("mtmobproxy"))
	require.NoError(t, err)

	src.Topic = "logs"
	parserConfigMap, err := parsers.ParserConfigStructToMap(new(blank.ParserConfigBlankLb))
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
			jsonparser.TransformerType: &jsonparser.Config{
				Parser: &json.ParserConfigJSONCommon{
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
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, src.Topic, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 50))
	require.NoError(t, w.Stop())
}
