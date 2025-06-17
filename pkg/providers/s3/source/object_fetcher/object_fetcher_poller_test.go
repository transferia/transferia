//go:build !disable_s3_provider

package objectfetcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher"
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
	}
	srcModel.WithDefaults()

	dpClient := testutil.NewFakeClientWithTransferState()

	transferID := "dtt"

	poller0Impl, err := NewObjectFetcherPoller(context.Background(), logger.Log, srcModel, fakeS3Client, dpClient, transferID, 0, 2, false)
	require.NoError(t, err)
	poller0 := NewObjectFetcherContractor(poller0Impl)

	poller1Impl, err := NewObjectFetcherPoller(context.Background(), logger.Log, srcModel, fakeS3Client, dpClient, transferID, 1, 2, false)
	require.NoError(t, err)
	poller1 := NewObjectFetcherContractor(poller1Impl)

	sess := fake_s3.NewSess()
	currReader, err := reader.New(srcModel, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, err)

	//------------------------------------------------------------------
	// init

	fakeS3Client.SetFiles(
		[]*fake_s3.File{
			fake_s3.NewFile("file_0", []byte("STUB"), 2),
		},
	)

	t.Run("check state after poller0.FetchAndCommitAll", func(t *testing.T) {
		err = poller0.FetchAndCommitAll(currReader)
		require.NoError(t, err)
		require.Equal(t, []string{"0"}, dpClient.StateKeys())
		require.Equal(t, `{"NS":0,"Files":[]}`, dpClient.GetTransferStateForTests(t)["0"].Generic)
	})

	t.Run("check state after poller1.FetchAndCommitAll", func(t *testing.T) {
		err = poller1.FetchAndCommitAll(currReader)
		require.NoError(t, err)
		require.Equal(t, []string{"0", "1"}, dpClient.StateKeys())
		require.Equal(t, `{"NS":2,"Files":["file_0"]}`, dpClient.GetTransferStateForTests(t)["1"].Generic)
	})

	//------------------------------------------------------------------
	// check if files divided between synthetic_partitions

	fakeS3Client.SetFiles(
		[]*fake_s3.File{
			fake_s3.NewFile("file_0", []byte("STUB"), 2),
			fake_s3.NewFile("file_1", []byte("STUB"), 2),
			fake_s3.NewFile("file_2", []byte("STUB"), 2),
			fake_s3.NewFile("file_3", []byte("STUB"), 2),
			fake_s3.NewFile("file_4", []byte("STUB"), 2),
		},
	)

	t.Run("check state after poller0.FetchAndCommitAll", func(t *testing.T) {
		err = poller0.FetchAndCommitAll(currReader)
		require.NoError(t, err)
		require.Equal(t, `{"NS":2,"Files":["file_4"]}`, dpClient.GetTransferStateForTests(t)["0"].Generic)
	})

	t.Run("check state after poller1.FetchAndCommitAll", func(t *testing.T) {
		err = poller1.FetchAndCommitAll(currReader)
		require.NoError(t, err)
		require.Equal(t, `{"NS":2,"Files":["file_0","file_1","file_2","file_3"]}`, dpClient.GetTransferStateForTests(t)["1"].Generic)
	})

	//------------------------------------------------------------------
	// check corner cases

	t.Run("add file to the past - no new objects", func(t *testing.T) {
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
		fakeS3Client.AddFile(fake_s3.NewFile("file_6", []byte("STUB"), 2))
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, []string{"file_6"}, objects)

		err = poller0.Commit("file_6")
		require.NoError(t, err)
	})

	fakeS3Client.AddFile(fake_s3.NewFile("file_7", []byte("STUB"), 3))
	fakeS3Client.AddFile(fake_s3.NewFile("file_8", []byte("STUB"), 4)) // not mine for poller0
	fakeS3Client.AddFile(fake_s3.NewFile("file_D", []byte("STUB"), 5))

	t.Run("commit two files in reverse order", func(t *testing.T) {
		objects, err := poller0.FetchObjects(currReader)
		require.NoError(t, err)
		require.Equal(t, []string{"file_7", "file_D"}, objects)

		require.Equal(t, `{"NS":2,"Files":["file_4","file_6"]}`, dpClient.GetTransferStateForTests(t)["0"].Generic) // CHECK IF STATE NOT CHANGED
		err = poller0.Commit("file_D")
		require.NoError(t, err)
		require.Equal(t, `{"NS":2,"Files":["file_4","file_6"]}`, dpClient.GetTransferStateForTests(t)["0"].Generic) // CHECK IF STATE NOT CHANGED

		err = poller0.Commit("file_7")
		require.NoError(t, err)
		require.Equal(t, `{"NS":5,"Files":["file_D"]}`, dpClient.GetTransferStateForTests(t)["0"].Generic) // CHECK IF STATE NOT CHANGED
	})
}

func TestInitDispatcherFromState(t *testing.T) {
	faceCpClient := testutil.NewFakeClientWithTransferState()
	transferID := "dtt"
	err := faceCpClient.SetTransferState(
		transferID,
		map[string]*coordinator.TransferStateData{
			"ReadProgressKey": {Generic: map[string]interface{}{
				"name":          "accounts/yandex/post_sale/V5.0/realtime/platform=desktop/year=2025/month=05/day=28/hour=18/08_output.parquet",
				"last_modified": "2025-05-28T18:09:21Z",
			}},
		},
	)
	require.NoError(t, err)

	currModel := &s3.S3Source{
		SyntheticPartitionsNum: 1,
	}
	workerProperties, err := dispatcher.NewWorkerProperties(0, 1)
	require.NoError(t, err)

	currDispatcher := dispatcher.NewDispatcher(currModel.SyntheticPartitionsNum, workerProperties)
	err = initDispatcherFromState(logger.Log, faceCpClient, currModel, transferID, currDispatcher)
	require.NoError(t, err)

	state := currDispatcher.SerializeState()
	zeroVal, ok := state["0"]
	require.True(t, ok)
	require.Equal(t, `{"NS":1748455761000000000,"Files":["accounts/yandex/post_sale/V5.0/realtime/platform=desktop/year=2025/month=05/day=28/hour=18/08_output.parquet"]}`, zeroVal.Generic.(string))
}
