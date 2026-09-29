package logbroker

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/parsers"
	blankparser "github.com/transferia/transferia/pkg/parsers/registry/blank"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestValidInstance(t *testing.T) {
	t.Setenv("YA_TEST_RUNNER", "0")
	consumer := "test_client"
	ydbDriver := ydbrecipe.Driver(t)
	topic := "fake-instance-topic"
	require.NoError(t, ydbDriver.Topic().Create(context.Background(), topic, topicoptions.CreateWithConsumer(topictypes.Consumer{
		Name:            consumer,
		SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
	})))

	_, port, db, creds := ydbrecipe.InstancePortDatabaseCreds(t)
	parserConfigMap, err := parsers.ParserConfigStructToMap(&blankparser.ParserConfigBlankLb{})
	require.NoError(t, err)

	cfg := &LfSource{
		Instance:    LogbrokerInstance("fakeinstance"),
		Port:        port,
		Database:    db,
		Topics:      []string{path.Join(db, topic)},
		Consumer:    consumer,
		Credentials: creds,

		ParserConfig: parserConfigMap,
	}
	cfg.WithDefaults()

	_, err = newOneDCSource(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.Error(t, err)
}
