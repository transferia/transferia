package gzip

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

const testCasePath = "test_gzip"

func buildSourceModel(t *testing.T) *s3.S3Source {
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
	src.InputFormat = dp_model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""
	return src
}

func testNativeS3(t *testing.T, src *s3.S3Source) {
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
	dst.WithDefaults()

	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, "people", "data", 500000)
}

func testNativeS3ManualSchemaWithPkey(t *testing.T, src *s3.S3Source) {
	sink := &helpers.MockSink{}
	sink.PushCallback = func(input []abstract.ChangeItem) error {
		for _, el := range input {
			if el.IsRowEvent() {
				fmt.Println("ROW_EVENT", el.ToJSONString())
				for _, currColSchema := range el.TableSchema.Columns() {
					if currColSchema.PrimaryKey {
						currColumnValue := el.ColumnValues[el.ColumnNameIndex(currColSchema.ColumnName)]
						if currColumnValue == nil {
							t.Fail()
						} else {
							return abstract.NewFatalError(xerrors.New("to immediately exit"))
						}
					}
				}
			}
		}
		return nil
	}
	dst := &dp_model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sink },
		Cleanup:       dp_model.DisabledCleanup,
	}

	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	_, err := helpers.ActivateErr(transfer)
	require.Error(t, err)
}

func TestAll(t *testing.T) {
	src := buildSourceModel(t)
	testNativeS3(t, src)
	helpers.TestS3SchemaAndPkeyCases(t, src, "Email", "")
}
