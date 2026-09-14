package snapshotjsonline

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

const testCasePath = "test_jsonline_large"

func buildSourceModel(t *testing.T) *s3_model.S3Source {
	src := s3recipe.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3recipe.CreateBucket(t, src)
		s3recipe.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "example"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatJSONLine
	src.WithDefaults()
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024
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
		Database:            "example",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
	dst.WithDefaults()
	transfer := transferhelpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, "example", "data", 500000)
}

func testNativeS3ManualSchemaWithPkey(t *testing.T, src *s3_model.S3Source) {
	sink := mocksink.NewMockSink(nil)
	sink.PushCallback = func(input []abstract.ChangeItem) error {
		for _, el := range input {
			if el.IsRowEvent() {
				fmt.Println("ROW_EVENT", el.ToJSONString())
				columnsWithPkey := 0
				for _, currColSchema := range el.TableSchema.Columns() {
					if currColSchema.PrimaryKey {
						columnsWithPkey++
						currColumnValue := el.ColumnValues[el.ColumnNameIndex(currColSchema.ColumnName)]
						if currColumnValue == nil {
							t.Fail()
						}
					}
				}
				require.Equal(t, 1, columnsWithPkey)
				return abstract.NewFatalError(xerrors.New("to immediately exit"))
			}
		}
		return nil
	}
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sink },
		Cleanup:       model.DisabledCleanup,
	}

	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	_, err := helpers.ActivateErr(transfer)
	require.Error(t, err)
}

func TestAll(t *testing.T) {
	src := buildSourceModel(t)
	testNativeS3(t, src)
	helpers.TestS3SchemaAndPkeyCases(t, src, "name", "")
}
