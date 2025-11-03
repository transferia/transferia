package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/coordinator/s3coordinator"
	"github.com/transferia/transferia/pkg/terryid"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/fake_sharding_storage"
)

type fakeSinker struct{}

func (f fakeSinker) Close() error {
	return nil
}

func (f fakeSinker) Push(input []abstract.ChangeItem) error {
	return nil
}

func TestShardedUploadCoordinator(t *testing.T) {
	cp, err := s3coordinator.NewS3Recipe(os.Getenv("S3_BUCKET"))
	require.NoError(t, err)
	ctx := context.Background()
	transfer := "dtt"

	tables := []abstract.TableDescription{
		{Schema: "sc", Name: "t1"},
		{Schema: "sc", Name: "t2"},
		{Schema: "sc", Name: "t3"},
		{Schema: "sc", Name: "t4"},
	}
	terminateSlave := func(taskID string, slaveID int, err error) error {
		logger.Log.Infof("%v: fake slave: %v finish", taskID, slaveID)
		return cp.FinishOperation(taskID, "", slaveID, err)
	}
	fakeSlaveProgress := func(taskID string, slaveID int, progress float64) (err error) {
		logger.Log.Infof("%v: fake slave: %v progress to %v", taskID, slaveID, progress)
		return err
	}
	t.Run("happy shard uploader", func(t *testing.T) {
		taskID := terryid.GenerateJobID()
		go func() {
			// slave 1
			time.Sleep(3 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 1, 10))
			time.Sleep(2 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 1, 35))
			time.Sleep(4 * time.Second)
			require.NoError(t, terminateSlave(taskID, 1, nil))
		}()
		go func() {
			// slave 2
			time.Sleep(5 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 2, 15))
			time.Sleep(3 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 2, 55))
			time.Sleep(7 * time.Second)
			require.NoError(t, terminateSlave(taskID, 2, nil))
		}()
		go func() {
			// slave 3
			time.Sleep(7 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 3, 23))
			time.Sleep(2 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 3, 75))
			time.Sleep(3 * time.Second)
			require.NoError(t, terminateSlave(taskID, 3, nil))
		}()
		transfer := &model.Transfer{
			ID: transfer,
			Runtime: &abstract.LocalRuntime{
				ShardingUpload: abstract.ShardUploadParams{JobCount: 3},
				CurrentJob:     0,
			},
			Src: &model.MockSource{
				StorageFactory: func() abstract.Storage {
					return fake_sharding_storage.NewFakeShardingStorage(tables)
				},
				AllTablesFactory: func() abstract.TableMap {
					return nil
				},
			},
			Dst: &model.MockDestination{
				SinkerFactory: func() abstract.Sinker {
					return &fakeSinker{}
				},
			},
		}
		snapshotLoader := tasks.NewSnapshotLoader(cp, taskID, transfer, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, false))
	})
	t.Run("sad shard uploader (one slave is dead)", func(t *testing.T) {
		taskID := terryid.GenerateJobID()
		go func() {
			// slave 1
			time.Sleep(3 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 1, 10))
			time.Sleep(2 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 1, 35))
			time.Sleep(15 * time.Second)
			require.NoError(t, terminateSlave(taskID, 1, nil))
		}()
		go func() {
			// slave 2
			time.Sleep(5 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 2, 15))
			time.Sleep(3 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 2, 55))
			time.Sleep(7 * time.Second)
			require.NoError(t, terminateSlave(taskID, 2, xerrors.New("woopsi-shmupsi, me is dead")))
		}()
		go func() {
			// slave 3
			time.Sleep(13 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 3, 23))
			time.Sleep(2 * time.Second)
			require.NoError(t, fakeSlaveProgress(taskID, 3, 75))
			time.Sleep(3 * time.Second)
			require.NoError(t, terminateSlave(taskID, 3, nil))
		}()
		transfer := &model.Transfer{
			ID: transfer,
			Runtime: &abstract.LocalRuntime{
				ShardingUpload: abstract.ShardUploadParams{JobCount: 3},
				CurrentJob:     0,
			},
			Src: &model.MockSource{
				StorageFactory: func() abstract.Storage {
					return fake_sharding_storage.NewFakeShardingStorage(tables)
				},
				AllTablesFactory: func() abstract.TableMap {
					return nil
				},
			},
			Dst: &model.MockDestination{
				SinkerFactory: func() abstract.Sinker {
					return &fakeSinker{}
				},
			},
		}
		snapshotLoader := tasks.NewSnapshotLoader(cp, taskID, transfer, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.Error(t, snapshotLoader.UploadTables(ctx, tables, false))
	})
}
