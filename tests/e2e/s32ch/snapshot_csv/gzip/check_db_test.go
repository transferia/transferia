package gzip

import (
	"os"
	"testing"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/tests/helpers"
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
	Database:            "people",
	HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	Cleanup:             dp_model.Drop,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "test_gzip"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "people"
	src.TableName = "data"
	src.InputFormat = dp_model.ParsingFormatCSV
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""
	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, "people", "data", 500000)
}
