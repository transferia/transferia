package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3"
)

func TestIncremental(t *testing.T) {
	testCasePath := "userdata"
	cfg := s3.PrepareCfg(t, "data4", "")
	cfg.PathPrefix = testCasePath
	// upload 2 files
	s3.UploadOne(t, cfg, "userdata/userdata1.parquet")
	time.Sleep(time.Second)
	betweenTime := time.Now()
	time.Sleep(time.Second)
	s3.UploadOne(t, cfg, "userdata/userdata2.parquet")
	logger.Log.Info("file uploaded")

	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	t.Run("no cursor", func(t *testing.T) {
		tables, err := storage.GetNextIncrementalState(context.Background(), []abstract.IncrementalTable{{
			Name:         cfg.TableName,
			Namespace:    cfg.TableNamespace,
			CursorField:  s3VersionCol,
			InitialState: "",
		}})
		require.NoError(t, err)
		require.Len(t, tables, 1)
		incrementState := abstract.IncrementalStateToTableDescription(tables)
		files, err := storage.ShardTable(context.Background(), incrementState[0])
		require.NoError(t, err)
		require.Equal(t, 0, len(files)) // no new files
	})
	t.Run("cursor in future", func(t *testing.T) {
		tables, err := storage.GetNextIncrementalState(context.Background(), []abstract.IncrementalTable{{
			Name:         cfg.TableName,
			Namespace:    cfg.TableNamespace,
			CursorField:  s3VersionCol,
			InitialState: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		}})
		require.NoError(t, err)
		require.Len(t, tables, 1)
		incrementState := abstract.IncrementalStateToTableDescription(tables)
		files, err := storage.ShardTable(context.Background(), incrementState[0])
		require.NoError(t, err)
		require.Equal(t, 0, len(files))
	})
	t.Run("cursor in past", func(t *testing.T) {
		tables, err := storage.GetNextIncrementalState(context.Background(), []abstract.IncrementalTable{{
			Name:         cfg.TableName,
			Namespace:    cfg.TableNamespace,
			CursorField:  s3VersionCol,
			InitialState: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		}})
		require.NoError(t, err)
		require.Len(t, tables, 1)
		incrementState := abstract.IncrementalStateToTableDescription(tables)
		files, err := storage.ShardTable(context.Background(), incrementState[0])
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
	})
	t.Run("cursor in between", func(t *testing.T) {
		tables, err := storage.GetNextIncrementalState(context.Background(), []abstract.IncrementalTable{{
			Name:         cfg.TableName,
			Namespace:    cfg.TableNamespace,
			CursorField:  s3VersionCol,
			InitialState: betweenTime.Format(time.RFC3339),
		}})
		require.NoError(t, err)
		require.Len(t, tables, 1)
		incrementState := abstract.IncrementalStateToTableDescription(tables)
		files, err := storage.ShardTable(context.Background(), incrementState[0])
		require.NoError(t, err)
		require.Equal(t, 1, len(files))
	})
}
