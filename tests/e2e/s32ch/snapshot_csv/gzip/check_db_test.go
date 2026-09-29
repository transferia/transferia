package gzip

import (
	"os"
	"testing"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
	"github.com/transferia/transferia/tests/helpers/s3"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

const testCasePath = "test_gzip"

func buildSourceModel(t *testing.T) *s3_model.S3Source {
	src := s3recipe.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3recipe.CreateBucket(t, src)
		s3recipe.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "people"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""
	return src
}

func testNativeS3(t *testing.T, src *s3_model.S3Source) {
	dst := clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
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
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
	dst.WithDefaults()

	transfer := transferhelpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	delivery.Activate(t, transfer)
	storagecomparison.CheckRowsCount(t, &dst, "people", "data", 500000)
}

func TestAll(t *testing.T) {
	src := buildSourceModel(t)
	testNativeS3(t, src)
	s3.TestS3SchemaAndPkeyCases(t, src, "Email", "")
}
