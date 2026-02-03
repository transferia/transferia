package object_fetcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	reader_factory "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/coordinator_utils"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestObjectFetcherPoller(t *testing.T) {
	fakeS3Client := fake_s3.NewFakeS3Client(t)

	srcModel := &s3.S3Source{
		InputFormat: model.ParsingFormatCSV,
		OutputSchema: []abstract.ColSchema{
			{ColumnName: "my_col", DataType: ytschema.TypeUint64.String()},
		},
		SyntheticPartitionsNum: 2,
		OverlapDuration:        3,
	}
	srcModel.WithDefaults()

	transferID := "dtt"
	dpClient := testutil.NewFakeClientWithTransferState()
	coordinatorStateAdapter := coordinator_utils.NewTransferStateAdapter(dpClient, 0, transferID)

	runtime := abstract.NewFakeShardingTaskRuntime(0, 777, 888, 2)
	effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, runtime, false)
	require.NoError(t, err)

	poller0Impl, err := newObjectFetcherPoller(context.Background(), logger.Log, srcModel, fakeS3Client, coordinatorStateAdapter, effectiveWorkerNum, r_window.NewRWindowEmpty(0))
	require.NoError(t, err)
	poller0 := NewObjectFetcherContractor(logger.Log, poller0Impl)

	sess := fake_s3.NewSess()
	currReader, err := reader_factory.NewReader(srcModel, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, err)

	//------------------------------------------------------------------
	// check if files divided between synthetic_partitions

	fakeS3Client.SetFiles(
		[]*fake_s3.File{
			fake_s3.NewFile("file_0", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_1", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_2", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_3", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_4", []byte("STUB"), 2),
		},
	)

	t.Run("CommitAll", func(t *testing.T) {
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, 1, len(objects))
		require.NoError(t, poller0.Commit(objects[0].FileName)) // file_4
		require.Equal(t, `{"ToHandle":{},"Handled":{"2":["file_4"]}}`, dpClient.GetTransferStateForTests(t)["0"].Generic)
	})

	//------------------------------------------------------------------
	// check corner cases

	t.Run("add file to the deep past (1000000000001 ns ago - it's 16.6min ago) - no new objects", func(t *testing.T) {
		fakeS3Client.AddFile(fake_s3.NewFile("file_5", []byte("STUB"), 1))
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, 0, len(objects))
	})

	t.Run("no changes - no new objects", func(t *testing.T) {
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, 0, len(objects))
	})

	t.Run("add file to the present - one new object", func(t *testing.T) {
		fakeS3Client.AddFile(fake_s3.NewFile("file_6", []byte("STUB"), 1000000000002))
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(
			t,
			[]file.File{
				{FileName: "file_6", FileSize: 0, LastModifiedNS: 1000000000002},
			},
			objects,
		)

		err = poller0.Commit("file_6")
		require.NoError(t, err)
		require.Equal(t, `{"ToHandle":{},"Handled":{"1000000000002":["file_6"],"2":["file_4"]}}`, dpClient.GetTransferStateForTests(t)["0"].Generic) // cleaned to TTL: {"2":["file_4"]}
	})

	fakeS3Client.AddFile(fake_s3.NewFile("file_7", []byte("STUB"), 1000000000003))
	fakeS3Client.AddFile(fake_s3.NewFile("file_8", []byte("STUB"), 1000000000004)) // not mine for poller0
	fakeS3Client.AddFile(fake_s3.NewFile("file_D", []byte("STUB"), 1000000000005))

	t.Run("commit two files in reverse order", func(t *testing.T) {
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(
			t,
			[]file.File{
				{FileName: "file_7", FileSize: 0, LastModifiedNS: 1000000000003},
				{FileName: "file_D", FileSize: 0, LastModifiedNS: 1000000000005},
			},
			objects,
		)

		require.Equal(t, `{"ToHandle":{},"Handled":{"1000000000002":["file_6"],"2":["file_4"]}}`, dpClient.GetTransferStateForTests(t)["0"].Generic)
		err = poller0.Commit("file_D")
		require.NoError(t, err)
		require.Equal(t, `{"ToHandle":{"1000000000003":["file_7"]},"Handled":{"1000000000002":["file_6"],"1000000000005":["file_D"],"2":["file_4"]}}`, dpClient.GetTransferStateForTests(t)["0"].Generic)

		err = poller0.Commit("file_7")
		require.NoError(t, err)
		require.Equal(t, `{"ToHandle":{},"Handled":{"1000000000002":["file_6"],"1000000000003":["file_7"],"1000000000005":["file_D"],"2":["file_4"]}}`, dpClient.GetTransferStateForTests(t)["0"].Generic)
	})
}

func TestFlush(t *testing.T) {
	transferID := "dtt"
	dpClient := testutil.NewFakeClientWithTransferState()
	coordinatorStateAdapter := coordinator_utils.NewTransferStateAdapter(dpClient, 10, transferID)

	var err error

	nsecToTime := func(nsec int64) time.Time {
		return time.Unix(0, nsec)
	}

	coordinatorStateAdapter.NowGetter = func() time.Time { return nsecToTime(1) }
	err = coordinatorStateAdapter.SetTransferState(
		map[string]*coordinator.TransferStateData{
			transferID: {
				Generic: "OLD",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, `OLD`, dpClient.GetTransferStateForTests(t)[transferID].Generic)

	coordinatorStateAdapter.NowGetter = func() time.Time { return nsecToTime(2) }
	err = coordinatorStateAdapter.SetTransferState(
		map[string]*coordinator.TransferStateData{
			transferID: {
				Generic: "NEW",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, `OLD`, dpClient.GetTransferStateForTests(t)[transferID].Generic)

	//---

	fakeS3Client := fake_s3.NewFakeS3Client(t)

	srcModel := &s3.S3Source{
		InputFormat: model.ParsingFormatCSV,
		OutputSchema: []abstract.ColSchema{
			{ColumnName: "my_col", DataType: ytschema.TypeUint64.String()},
		},
		SyntheticPartitionsNum: 2,
		OverlapDuration:        3,
	}
	srcModel.WithDefaults()

	runtime := abstract.NewFakeShardingTaskRuntime(0, 777, 888, 2)
	effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, runtime, false)
	require.NoError(t, err)

	poller0Impl, err := newObjectFetcherPoller(context.Background(), logger.Log, srcModel, fakeS3Client, coordinatorStateAdapter, effectiveWorkerNum, r_window.NewRWindowEmpty(0))
	require.NoError(t, err)
	poller0 := NewObjectFetcherContractor(logger.Log, poller0Impl)

	sess := fake_s3.NewSess()
	currReader, err := reader_factory.NewReader(srcModel, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, err)

	fakeS3Client.SetFiles(
		[]*fake_s3.File{
			fake_s3.NewFile("file_0", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_1", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_2", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_3", []byte("STUB"), 1000000000002),
			fake_s3.NewFile("file_4", []byte("STUB"), 2),
			fake_s3.NewFile("file_5", []byte("STUB"), 1),
		},
	)

	t.Run("CommitAll", func(t *testing.T) {
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, 2, len(objects))
	})
}
