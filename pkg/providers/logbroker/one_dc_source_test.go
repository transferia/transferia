package logbroker

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtypes"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	blankparser "github.com/transferia/transferia/pkg/parsers/registry/blank"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb/recipe"
	ydbtopic "github.com/transferia/transferia/tests/helpers/ydb/recipe/topic"
)

func TestDiscoveredReaderReadsAllAccounts(t *testing.T) {
	for _, topicAPI := range []bool{false, true} {
		t.Run(fmt.Sprintf("TopicAPI=%t", topicAPI), func(t *testing.T) {
			driver := ydbrecipe.Driver(t)
			defer func() { require.NoError(t, driver.Close(context.Background())) }()
			instance, port, database, creds := ydbrecipe.InstancePortDatabaseCreds(t)
			database = "/" + strings.TrimLeft(database, "/")
			topics := make([]string, 0, 2)
			for _, name := range []string{"a", "b"} {
				account := fmt.Sprintf("discovered_%s_%t", name, topicAPI)
				require.NoError(t, driver.Scheme().MakeDirectory(context.Background(), path.Join(database, account)))
				topic := account + "/topic"
				ydbtopic.CreateAndFillTopicWithDriver(t, topic, [][]byte{[]byte(topic)}, driver)
				topics = append(topics, topic)
			}
			parserConfig, err := parsers.ParserConfigStructToMap(&blankparser.ParserConfigBlankLb{})
			require.NoError(t, err)
			cfg := &LfSource{Cluster: Logbroker, Database: database, Topics: topics, Consumer: ydbtopic.DefaultConsumer,
				Credentials: creds, ParserConfig: parserConfig, UseTopicAPI: topicAPI, ParseQueueParallelism: 1}
			cfg.WithDefaults()
			instances, err := resolveInstancesWithDiscovery(context.Background(), cfg, logger.Log, func(context.Context, topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
				return []fedtypes.DatabaseInfo{{Endpoint: topiccommon.FormatEndpoint(instance, port), Path: "/not-the-reader-database"}}, nil
			})
			require.NoError(t, err)
			require.Len(t, instances, 1)
			multi := newControlledMultiSource(t)
			multi.cfg = cfg
			child := multi.configForInstance(instances[0])
			source, err := newOneDCSource(context.Background(), &child, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
			require.NoError(t, err)
			defer source.Stop()
			rows := make(chan abstract.ChangeItem, 4)
			sink := mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
				for _, item := range items {
					if item.IsRowEvent() {
						select {
						case rows <- item:
						default:
						}
					}
				}
				return nil
			})
			done := make(chan error, 1)
			go func() { done <- source.Run(sink) }()
			received := make(map[string]string)
			timer := time.NewTimer(15 * time.Second)
			defer timer.Stop()
			for len(received) < len(topics) {
				select {
				case item := <-rows:
					message := string(item.ColumnValues[blankparser.BlankColsIDX[blankparser.RawMessageColumn]].([]byte))
					received[message] = item.Table
				case err := <-done:
					t.Fatalf("reader stopped before delivering all topics: %v", err)
				case <-timer.C:
					t.Fatal("reader did not deliver data from both accounts")
				}
			}
			for _, topic := range topics {
				require.Equal(t, abstract.NewPartition(topic, 0).String(), received[topic])
			}
			source.Stop()
			select {
			case err := <-done:
				if err != nil {
					require.ErrorIs(t, err, eventreader.ErrClosedReader)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("reader did not stop")
			}
		})
	}
}
