package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = yt_provider.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		Proxy:            os.Getenv("YT_PROXY"),
		Paths:            []string{"//table_for_tests"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	Target = model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "default",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		SSLEnabled:          false,
		Cleanup:             dp_model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

type numColStats struct {
	MinValue string `json:"min_value"`
	MaxValue string `json:"max_value"`
	UniqCnt  string `json:"uniq_cnt"`
}

type tableRow struct {
	RowIdx     string `json:"row_idx"` // CH JSON output for Int64 is string
	SomeNumber string `json:"some_number"`
	TextVal    string `json:"text_val"`
	YsonVal    string `json:"yson_val"`
}

func init() {
	_ = os.Setenv("YT_LOG_LEVEL", "trace")
}

func TestBigTable(t *testing.T) {
	// defer require.NoError(t, helpers.CheckConnections(, Target.NativePort))

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

	storageParams, err := Target.ToStorageParams()
	require.NoError(t, err)
	chClient, err := httpclient.NewHTTPClientImpl(storageParams.ToConnParams())
	require.NoError(t, err)

	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.Proxy, Token: Source.YtToken})
	require.NoError(t, err)

	var rowCount int
	err = ytc.GetNode(context.Background(), ypath.NewRich(Source.Paths[0]).YPath().Attr("row_count"), &rowCount, nil)
	require.NoError(t, err)

	host := storageParams.ConnectionParams.Hosts[0]
	query := `
		SELECT
			min(some_number) as min_value,
			max(some_number) as max_value,
			uniqExact(some_number) as uniq_cnt
		FROM table_for_tests
		FORMAT JSONEachRow`
	var res numColStats
	err = chClient.Query(context.Background(), logger.Log, host, query, &res)
	require.NoError(t, err)

	require.Equal(t, "1", res.MinValue)
	require.Equal(t, strconv.Itoa(rowCount), res.MaxValue)
	require.Equal(t, strconv.Itoa(rowCount), res.UniqCnt)

	query = `
		SELECT
			min(row_idx) as min_value,
			max(row_idx) as max_value,
			uniqExact(row_idx) as uniq_cnt
		FROM table_for_tests
		FORMAT JSONEachRow`
	err = chClient.Query(context.Background(), logger.Log, host, query, &res)
	require.NoError(t, err)

	require.Equal(t, "0", res.MinValue)
	require.Equal(t, strconv.Itoa(rowCount-1), res.MaxValue)
	require.Equal(t, strconv.Itoa(rowCount), res.UniqCnt)

	query = `
		SELECT
			row_idx,
			some_number,
			text_val,
			yson_val
		FROM table_for_tests
		ORDER BY rand()
		LIMIT 1000
		FORMAT JSONEachRow`

	body, err := chClient.QueryStream(context.Background(), logger.Log, host, query)
	require.NoError(t, err)
	b, err := io.ReadAll(body)
	require.NoError(t, err)

	for _, r := range bytes.Split(b, []byte("\n")) {
		if len(r) == 0 {
			// skip empty last string
			continue
		}
		var dataRow tableRow
		err := json.Unmarshal(r, &dataRow)
		require.NoError(t, err)

		rowIdx, err := strconv.Atoi(dataRow.RowIdx)
		require.NoError(t, err)

		expectedNum := rowIdx
		if rowIdx%2 == 0 {
			expectedNum = rowCount - rowIdx
		}
		require.Equal(t, strconv.Itoa(expectedNum), dataRow.SomeNumber)
		require.Equal(t, fmt.Sprintf("sample %d text", rowIdx), dataRow.TextVal)
		var ysonData map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(dataRow.YsonVal), &ysonData))
		require.Equal(t, 1, len(ysonData))
		require.Equal(t, fmt.Sprintf("value_%d", rowIdx), ysonData["key"])
	}
}
