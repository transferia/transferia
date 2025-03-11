package gzip

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var dst = model.ChDestination{
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
	Database:            "test",
	HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	Cleanup:             dp_model.Drop,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "test_csv_replication_gzip"
	src := s3.PrepareCfg(t, "data4", "")
	src.PathPrefix = testCasePath

	s3.UploadOne(t, src, "test_csv_replication_gzip/test_1.csv.gz")
	time.Sleep(time.Second)

	src.TableNamespace = "test"
	src.TableName = "data"
	src.InputFormat = dp_model.ParsingFormatCSV
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""

	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	helpers.Activate(t, transfer)

	err := helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 12)
	require.NoError(t, err)

	s3.UploadOne(t, src, "test_csv_replication_gzip/test_2.csv.gz")
	time.Sleep(time.Second)

	err = helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 24)
	require.NoError(t, err)

	s3.UploadOne(t, src, "test_csv_replication_gzip/test_3.csv.gz")
	time.Sleep(time.Second)

	err = helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 36)
	require.NoError(t, err)

	s3.UploadOne(t, src, "test_csv_replication_gzip/test_4.csv.gz")
	time.Sleep(time.Second)

	err = helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 48)
	require.NoError(t, err)

	s3.UploadOne(t, src, "test_csv_replication_gzip/test_5.csv.gz")
	time.Sleep(time.Second)

	err = helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 60*time.Second, 60)
	require.NoError(t, err)
}
