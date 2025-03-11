package snapshotjsonline

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
	Database:            "example",
	HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	Cleanup:             dp_model.Drop,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "test_jsonline_large"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "example"
	src.TableName = "data"
	src.InputFormat = dp_model.ParsingFormatJSONLine
	src.WithDefaults()
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024
	dst.WithDefaults()
	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, "example", "data", 500000)
}
