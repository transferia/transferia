package sharding_storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/s3"
	s3provider "github.com/transferia/transferia/pkg/providers/s3/provider"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	providers.Register(s3.ProviderType, s3provider.New)
}

const line0 = `{"Item":{"OrderID":{"S":"0"},"OrderDate":{"S":"2023-07-01T12:00:00Z"},"CustomerName":{"S":"John Doe0"},"OrderAmount":{"N":"3540"}}}
`

const line1 = `{"Item":{"OrderID":{"S":"1"},"OrderDate":{"S":"2023-07-01T12:00:00Z"},"CustomerName":{"S":"John Doe1"},"OrderAmount":{"N":"3540"}}}
`

func buildSourceModel(t *testing.T) *s3.S3Source {
	src := s3recipe.PrepareCfg(t, "", "")
	src.TableNamespace = "example"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatJSON
	src.Format.JSONLSetting = new(s3.JSONLSetting)
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024
	src.OutputSchema = []abstract.ColSchema{
		{ColumnName: "OrderID", DataType: ytschema.TypeString.String(), Path: "Item.OrderID.S", PrimaryKey: true},
		{ColumnName: "OrderDate", DataType: ytschema.TypeDatetime.String(), Path: "Item.OrderDate.S"},
		{ColumnName: "CustomerName", DataType: ytschema.TypeString.String(), Path: "Item.CustomerName.S"},
		{ColumnName: "CustomerAmount", DataType: ytschema.TypeInt32.String(), Path: "Item.OrderAmount.N"},
	}
	src.WithDefaults()
	return src
}

func TestShardingTransfer(t *testing.T) {
	cfg := buildSourceModel(t)
	task := &model.TransferOperation{}
	task.OperationID = "dtj"

	s3recipe.UploadOneFromMemory(t, cfg, "file_0.jsonl", []byte(line0))
	s3recipe.UploadOneFromMemory(t, cfg, "file_1.jsonl", []byte(line1))

	var result []abstract.ChangeItem
	var mockSinkMutex sync.Mutex
	mockSink := mocksink.NewMockSink(func(in []abstract.ChangeItem) error {
		mockSinkMutex.Lock()
		defer mockSinkMutex.Unlock()
		for _, changeItem := range in {
			fmt.Printf("QQQ::MockSink::changeItem=%s\n", changeItem.ToJSONString())
			result = append(result, changeItem)
		}
		return nil
	})

	cp := coordinator.NewStatefulFakeClient()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		transferMain := &model.Transfer{
			Src: cfg,
			Dst: &model.MockDestination{
				SinkerFactory: func() abstract.Sinker {
					return mockSink
				},
			},
			Runtime: &abstract.LocalRuntime{
				CurrentJob:     0, // 'MAIN' worker
				ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1},
			},
		}
		snapshotLoader := tasks.NewSnapshotLoader(cp, task, transferMain, solomon.NewRegistry(nil))
		ctx := context.Background()
		err := snapshotLoader.UploadTables(ctx, []abstract.TableDescription{{Name: "test"}}, false)
		require.NoError(t, err)
	}()

	time.Sleep(3 * time.Second)

	go func() {
		defer wg.Done()
		transferJob1 := &model.Transfer{
			Src: cfg,
			Dst: &model.MockDestination{
				SinkerFactory: func() abstract.Sinker {
					return mockSink
				},
			},
			Runtime: &abstract.LocalRuntime{
				CurrentJob:     1, // 'SECONDARY' worker
				ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1},
			},
		}
		snapshotLoader := tasks.NewSnapshotLoader(cp, task, transferJob1, solomon.NewRegistry(nil))
		ctx := context.Background()
		err := snapshotLoader.UploadTables(ctx, []abstract.TableDescription{{Name: "test"}}, false)
		require.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		transferJob2 := &model.Transfer{
			Src: cfg,
			Dst: &model.MockDestination{
				SinkerFactory: func() abstract.Sinker {
					return mockSink
				},
			},
			Runtime: &abstract.LocalRuntime{
				CurrentJob:     2, // 'SECONDARY' worker
				ShardingUpload: abstract.ShardUploadParams{JobCount: 2, ProcessCount: 1},
			},
		}
		snapshotLoader := tasks.NewSnapshotLoader(cp, task, transferJob2, solomon.NewRegistry(nil))
		ctx := context.Background()
		err := snapshotLoader.UploadTables(ctx, []abstract.TableDescription{{Name: "test"}}, false)
		require.NoError(t, err)
	}()

	wg.Wait()

	line1Found := false
	line2Found := false
	rowEventsNum := 0
	for _, changeItem := range result {
		if strings.Contains(changeItem.ToJSONString(), `John Doe0`) {
			line1Found = true
		}
		if strings.Contains(changeItem.ToJSONString(), `John Doe1`) {
			line2Found = true
		}
		if changeItem.IsRowEvent() {
			rowEventsNum++
		}
	}
	require.Equal(t, 2, rowEventsNum)
	require.True(t, line1Found && line2Found)
}
