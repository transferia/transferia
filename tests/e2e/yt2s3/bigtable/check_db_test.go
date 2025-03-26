package snapshot

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	transferType = abstract.TransferTypeSnapshotOnly
	source       = &yt_provider.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		Proxy:            os.Getenv("YT_PROXY"),
		Paths:            []string{"//table_for_tests"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
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
	target := s3.PrepareS3(t, t.Name(), dp_model.ParsingFormatJSON, s3.NoEncoding)
	helpers.InitSrcDst(helpers.TransferID, source, target, transferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, transferType)
	helpers.Activate(t, transfer)

	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: source.Proxy, Token: source.YtToken})
	require.NoError(t, err)

	var rowCount int64
	err = ytc.GetNode(context.Background(), ypath.NewRich(source.Paths[0]).YPath().Attr("row_count"), &rowCount, nil)
	require.NoError(t, err)

	s3Src := &s3.S3Source{
		Bucket:           target.Bucket,
		ConnectionConfig: target.ConnectionConfig(),
		PathPrefix:       "",
		HideSystemCols:   false,
		ReadBatchSize:    0,
		InflightLimit:    0,
		TableName:        "test",
		TableNamespace:   "",
		InputFormat:      dp_model.ParsingFormatJSONLine,
		OutputSchema: []changeitem.ColSchema{
			changeitem.NewColSchema("row_idx", schema.TypeInt64, false),
			changeitem.NewColSchema("some_number", schema.TypeInt64, false),
			changeitem.NewColSchema("text_val", schema.TypeString, false),
			changeitem.NewColSchema("yson_val", schema.TypeAny, false),
		},
		AirbyteFormat:  "",
		PathPattern:    "",
		Concurrency:    0,
		Format:         s3.Format{},
		EventSource:    s3.EventSource{},
		UnparsedPolicy: "",
	}

	var mu sync.Mutex
	var totalCnt int64
	minVal := int64(math.MaxInt64)
	maxVal := int64(math.MinInt64)
	var someItem changeitem.ChangeItem
	sinkMock := &helpers.MockSink{
		PushCallback: func(items []changeitem.ChangeItem) {
			mu.Lock()
			defer mu.Unlock()
			for _, item := range items {
				if !item.IsRowEvent() {
					continue
				}

				values := item.AsMap()
				if v := values["some_number"].(int64); v > maxVal {
					maxVal = v
				}
				if v := values["some_number"].(int64); v < minVal {
					minVal = v
				}
				someItem = item
				totalCnt++
			}
		},
	}
	targetMock := &dp_model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       dp_model.DisabledCleanup,
	}
	helpers.InitSrcDst(helpers.TransferID, s3Src, targetMock, transferType)
	transfer = helpers.MakeTransfer(helpers.TransferID, s3Src, targetMock, transferType)
	helpers.Activate(t, transfer)

	require.Equal(t, int64(1), minVal)
	require.Equal(t, rowCount, maxVal)
	require.Equal(t, rowCount, totalCnt)

	itemValues := someItem.AsMap()
	rowIdx := itemValues["row_idx"].(int64)

	expectedNum := rowIdx
	if rowIdx%2 == 0 {
		expectedNum = rowCount - rowIdx
	}
	require.Equal(t, expectedNum, itemValues["some_number"])

	require.Equal(t, fmt.Sprintf("sample %d text", rowIdx), itemValues["text_val"])
	ysonData := itemValues["yson_val"].(map[string]any)
	require.Equal(t, 1, len(ysonData))
	require.Equal(t, fmt.Sprintf("value_%d", rowIdx), ysonData["key"])
}
