package polling

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var dst = clickhouse_model.ChDestination{
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
	Database:            "test",
	HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	Cleanup:             model.Drop,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "thousands_of_csv_files"
	src := s3recipe.PrepareCfg(t, "data4", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3recipe.UploadOne(t, src, "thousands_of_csv_files/data0.csv")
		//s3.PrepareTestCase(t, src, src.PathPrefix)
	}

	time.Sleep(5 * time.Second)

	src.TableNamespace = "test"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.CSVSetting.BlockSize = 10000000
	src.ReadBatchSize = 4000 // just for testing so its faster, normally much smaller
	src.Format.CSVSetting.QuoteChar = "\""

	start := time.Now()
	transfer := transferhelpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	delivery.Activate(t, transfer)

	for i := 1; i < 1240; i++ {
		s3recipe.UploadOne(t, src, fmt.Sprintf("thousands_of_csv_files/data%d.csv", i))
	}

	err := storage.WaitDestinationEqualRowsCount("test", "data", storagecomparison.GetSampleableStorageByModel(t, transfer.Dst), 500*time.Second, 426216)
	require.NoError(t, err)
	finish := time.Now()

	duration := finish.Sub(start)
	fmt.Println("Execution took:", duration)
}
