package jsonlogs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/logging"
	"github.com/transferia/transferia/pkg/parsers"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	yds_source "github.com/transferia/transferia/pkg/providers/yds/source"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/topicwriter"
	"github.com/transferia/transferia/tests/helpers"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb_recipe/topic"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const (
	topicName = "src/topic"
	tableName = "topic"
)

func TestPushClientLogs(t *testing.T) {
	cfg := &yt.Config{}
	ytProxy, err := cfg.GetProxy()
	require.NoError(t, err)

	ytEnv, cancel := yttest.NewEnv(t)

	instance, port, db, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	ydbtopic.CreateTopic(t, topicName, ydbrecipe.Driver(t))

	sourcePort := port
	targetPort, err := helpers.GetPortFromStr(ytProxy)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDBTopic source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()
	defer cancel()

	lgr, err := logger.NewLogbrokerLoggerFromConfig(&topicwriter.Config{
		Instance:    instance,
		Port:        port,
		Database:    db,
		Topic:       topicName,
		SourceID:    "test",
		Credentials: creds,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()), logging.LogLevel())
	require.NoError(t, err)

	parserConfigStruct := &parser_json.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &yds_source.YDBTopicSource{
		Endpoint:     fmt.Sprintf("%s:%d", instance, port),
		Database:     db,
		Topics:       []string{topicName},
		Credentials:  creds,
		Consumer:     ydbtopic.DefaultConsumer,
		ParserConfig: parserConfigMap,
	}
	src.WithDefaults()
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/logs_e2e",
		Cluster:       ytProxy,
		Token:         cfg.GetToken(),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	dst.WithDefaults()

	transfer := &model.Transfer{
		ID:  "e2e_test",
		Src: src,
		Dst: dst,
	}

	go func() {
		for i := 0; i < 50; i++ {
			lgr.Infof("line:%v", i)
		}
	}()
	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	localWorker.Start()
	defer func() {
		err := localWorker.Stop()
		require.NoError(t, ytEnv.YT.RemoveNode(context.TODO(), ypath.Path("//home/cdc/test/logs_e2e"), &yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		}))
		require.NoError(t, err)
	}()

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", tableName, helpers.GetSampleableStorageByModel(t, dst.LegacyModel()), 60*time.Second, 50))
}
