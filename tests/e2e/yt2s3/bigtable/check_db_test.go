package snapshot

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/terryid"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	transferType = abstract.TransferTypeSnapshotOnly
	source       = &yt_provider.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		YtProxy:          os.Getenv("YT_PROXY"),
		Paths:            []string{"//table_for_tests"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	taskCreatedAt = time.Date(2026, 1, 3, 3, 7, 6, 7, time.UTC)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	_ = os.Setenv("YT_LOG_LEVEL", "trace")
}

func runSecondaryWorker(ctx context.Context, t *testing.T, cp coordinator.Coordinator, transfer *dp_model.Transfer, operationID string, workerIndex int, wg *sync.WaitGroup) {
	defer wg.Done()

	time.Sleep(3 * time.Second)

	secondaryTransfer := *transfer
	secondaryTransfer.Runtime = &abstract.LocalRuntime{
		Host:           "",
		CurrentJob:     workerIndex,
		ShardingUpload: transfer.Runtime.(*abstract.LocalRuntime).ShardingUpload,
	}

	task := &dp_model.TransferOperation{
		OperationID: operationID,
		TransferID:  transfer.ID,
		TaskType:    abstract.TaskType{Task: abstract.Activate{}},
		Status:      dp_model.RunningTask,
		CreatedAt:   taskCreatedAt,
	}

	logger.Log.Infof("Starting secondary worker %d for operation %s", workerIndex, operationID)
	err := tasks.ActivateDelivery(ctx, task, cp, secondaryTransfer, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	if finishErr := cp.FinishOperation(operationID, task.TaskType.String(), "", workerIndex, err); finishErr != nil {
		logger.Log.Warnf("Failed to finish operation for worker %d: %v", workerIndex, finishErr)
		require.NoError(t, finishErr)
	}

	if err != nil && !xerrors.Is(err, context.Canceled) {
		logger.Log.Errorf("Secondary worker %d failed: %v", workerIndex, err)
		require.NoError(t, err)
	}

	logger.Log.Infof("Secondary worker %d completed", workerIndex)
}

func TestBigTable(t *testing.T) {
	target := s3recipe.PrepareS3(t, t.Name(), dp_model.ParsingFormatJSON, s3.NoEncoding)
	helpers.InitSrcDst(helpers.TransferID, source, target, transferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, transferType)
	helpers.Activate(t, transfer)

	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: source.YtProxy, Token: source.YtToken})
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
	sinkMock := mocksink.NewMockSink(func(items []changeitem.ChangeItem) error {
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
		return nil
	})
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

func TestBigTableWithParallelWorkers(t *testing.T) {
	source.DesiredPartSizeBytes = 1024 * 1024 // 1MB
	target := s3recipe.PrepareS3(t, t.Name(), dp_model.ParsingFormatJSON, s3.NoEncoding)
	target.MaxItemsPerFile = 30000
	helpers.InitSrcDst(helpers.TransferID, source, target, transferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, transferType)

	workersCount := 2
	transfer = helpers.WithLocalRuntime(transfer, workersCount, 2)

	cp := coordinator.NewStatefulFakeClient()

	operationID := terryid.GenerateJobID()
	logger.Log.Infof("Generated operation ID: %s", operationID)

	var wg sync.WaitGroup
	ctx := context.Background()
	for i := 1; i <= workersCount; i++ {
		wg.Add(1)
		go runSecondaryWorker(ctx, t, cp, transfer, operationID, i, &wg)
	}

	task := &dp_model.TransferOperation{
		OperationID: operationID,
		TransferID:  transfer.ID,
		TaskType:    abstract.TaskType{Task: abstract.Activate{}},
		Status:      dp_model.RunningTask,
		CreatedAt:   taskCreatedAt,
	}

	logger.Log.Info("Starting main worker")
	err := tasks.ActivateDelivery(ctx, task, cp, *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	wg.Wait()
	logger.Log.Info("All workers completed")

	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: source.YtProxy, Token: source.YtToken})
	require.NoError(t, err)

	var rowCount int64
	err = ytc.GetNode(context.Background(), ypath.NewRich(source.Paths[0]).YPath().Attr("row_count"), &rowCount, nil)
	require.NoError(t, err)

	s3Src := &s3.S3Source{
		Bucket:           target.Bucket,
		ConnectionConfig: target.ConnectionConfig(),
		PathPrefix:       "",
		HideSystemCols:   false,
		ReadBatchSize:    100000,
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
	sinkMock := &mocksink.MockSink{
		PushCallback: func(items []changeitem.ChangeItem) error {
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
			return nil
		},
	}
	targetMock := &dp_model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       dp_model.DisabledCleanup,
	}

	logger.Log.Info("start transfer from s3 to mock sink")
	helpers.InitSrcDst(helpers.TransferID, s3Src, targetMock, transferType)
	transfer = helpers.MakeTransfer(helpers.TransferID, s3Src, targetMock, transferType)
	helpers.Activate(t, transfer)

	logger.Log.Info("end transfer from s3 to mock sink")
	logger.Log.Infof("totalCnt: %d", totalCnt)

	require.Equal(t, int64(1), minVal)
	require.Equal(t, totalCnt, int64(1000000))
	require.Equal(t, rowCount, totalCnt)

	itemValues := someItem.AsMap()
	rowIdx := itemValues["row_idx"].(int64)

	expectedNum := rowIdx
	if rowIdx%2 == 0 {
		expectedNum = rowCount - rowIdx
	}
	logger.Log.Infof("expectedNum: %d", expectedNum)
	logger.Log.Infof("itemValues: %v", itemValues["some_number"])
	require.Equal(t, expectedNum, itemValues["some_number"].(int64))

	require.Equal(t, fmt.Sprintf("sample %d text", rowIdx), itemValues["text_val"])
	ysonData := itemValues["yson_val"].(map[string]any)
	require.Equal(t, 1, len(ysonData))
	require.Equal(t, fmt.Sprintf("value_%d", rowIdx), ysonData["key"])
	logger.Log.Infof("ysonData: %v", ysonData)
}
