//go:build !disable_s3_provider

package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
)

func TestDefaultShardingWithBlob(t *testing.T) {
	testCasePath := "yellow_taxi"
	cfg := s3.PrepareCfg(t, "blobiki_bobiki", model.ParsingFormatPARQUET)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	tid := *abstract.NewTableID(cfg.TableNamespace, cfg.TableName)
	t.Run("single blob", func(t *testing.T) {
		cfg.PathPattern = "*2023*" // only include 2023 year.
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 2)
	})
	t.Run("all", func(t *testing.T) {
		cfg.PathPattern = "*" // all files
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 4)
	})
	t.Run("or case", func(t *testing.T) {
		cfg.PathPattern = "*2023*|*2022-12*" // 2023 and one month of 2022
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 3)
	})
}

func createFiles(t *testing.T, filesNumber, fileSize int) string {
	dir := t.TempDir()
	for i := range filesNumber {
		randBytes := make([]byte, fileSize-1)
		_, err := rand.Read(randBytes)
		require.NoError(t, err)
		file, err := os.Create(fmt.Sprintf("%s/file-%d", dir, i))
		require.NoError(t, err)
		_, err = file.Write(randBytes)
		require.NoError(t, err)
		_, err = file.Write([]byte{'\n'})
		require.NoError(t, err)
	}
	return dir
}

func TestCustomSharding(t *testing.T) {
	filesNumber := 100
	fileSize := 100 * humanize.Byte

	cfg := s3.PrepareCfg(t, "data3", model.ParsingFormatLine)
	cfg.PathPattern = "*"
	cfg.PathPrefix = createFiles(t, filesNumber, fileSize)
	if os.Getenv("S3MDS_PORT") != "" {
		logger.Log.Infof("dir %s uploading...", cfg.PathPrefix)
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Infof("dir %s uploaded", cfg.PathPrefix)
	}
	cfg.PathPrefix = strings.TrimLeft(cfg.PathPrefix, "/")
	tid := *abstract.NewTableID(cfg.TableNamespace, cfg.TableName)

	type testCase struct {
		params   *s3.ShardingParams
		expected int
	}
	cases := []testCase{
		{
			params: &s3.ShardingParams{
				PartBytesLimit: 0,
				PartFilesLimit: 0,
			},
			expected: 100,
		},
		{
			params: &s3.ShardingParams{
				PartBytesLimit: uint64(fileSize * filesNumber / 10),
				PartFilesLimit: math.MaxInt,
			},
			expected: 10,
		},
		{
			params: &s3.ShardingParams{
				PartBytesLimit: math.MaxInt,
				PartFilesLimit: uint64(filesNumber / 10),
			},
			expected: 10,
		},
		{
			params: &s3.ShardingParams{
				PartBytesLimit: uint64(fileSize * filesNumber),
				PartFilesLimit: uint64(filesNumber),
			},
			expected: 1,
		},
	}

	for i, testCase := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			cfg.ShardingParams = testCase.params
			storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
			require.NoError(t, err)
			files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
			require.NoError(t, err)
			require.Equal(t, testCase.expected, len(files))
		})
	}
}
