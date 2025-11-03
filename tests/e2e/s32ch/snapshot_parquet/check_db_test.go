package mssql

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

const testCasePath = "fhv_taxi"

func buildSourceModel(t *testing.T) *s3.S3Source {
	src := s3recipe.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data3"
		s3recipe.CreateBucket(t, src)
		s3recipe.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "taxi"
	src.TableName = "trip"
	return src
}

func testNativeS3(t *testing.T, src *s3.S3Source) {
	target := model.ChDestination{
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
		Database:            "taxi",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		ChClusterName:       "test_shard_localhost",
		Cleanup:             dp_model.Truncate,
	}
	target.WithDefaults()
	transfer := helpers.MakeTransfer("fake", src, &target, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &target, "taxi", "trip", 2439039)
}

func testNativeS3ManualSchemaWithPkey(t *testing.T, src *s3.S3Source) {
	sink := &helpers.MockSink{}
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
	helpers.TestS3SchemaAndPkeyCases(t, src, "Affiliated_base_number", "")
}
