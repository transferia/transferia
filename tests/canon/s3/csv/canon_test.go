package csv

import (
	_ "embed"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/tests/canon/validator"
	"github.com/transferria/transferria/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	testCasePath := "test_csv_all_types"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "test"
	src.TableName = "types"
	src.Format.CSVSetting = new(s3.CSVSetting)
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""
	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.HideSystemCols = true
	src.OutputSchema = []abstract.ColSchema{
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "0",
			DataType:    schema.TypeBoolean.String(),
			ColumnName:  "boolean",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "1",
			DataType:    schema.TypeUint8.String(),
			ColumnName:  "uint8",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "2",
			DataType:    schema.TypeUint16.String(),
			ColumnName:  "uint16",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "3",
			DataType:    schema.TypeUint32.String(),
			ColumnName:  "uint32",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "4",
			DataType:    schema.TypeUint64.String(),
			ColumnName:  "uint64",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "5",
			DataType:    schema.TypeInt8.String(),
			ColumnName:  "int8",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "6",
			DataType:    schema.TypeInt16.String(),
			ColumnName:  "int16",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "7",
			DataType:    schema.TypeInt32.String(),
			ColumnName:  "int32",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "8",
			DataType:    schema.TypeInt64.String(),
			ColumnName:  "int64",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "9",
			DataType:    schema.TypeFloat32.String(),
			ColumnName:  "float32",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "10",
			DataType:    schema.TypeFloat64.String(),
			ColumnName:  "float64",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "11",
			DataType:    schema.TypeBytes.String(),
			ColumnName:  "bytes",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "12",
			DataType:    schema.TypeString.String(),
			ColumnName:  "string",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "13",
			DataType:    schema.TypeDate.String(),
			ColumnName:  "date",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "14",
			DataType:    schema.TypeDatetime.String(),
			ColumnName:  "dateTime",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "15",
			DataType:    schema.TypeTimestamp.String(),
			ColumnName:  "timestamp",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "16",
			DataType:    schema.TypeInterval.String(),
			ColumnName:  "interval",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "17",
			DataType:    schema.TypeAny.String(),
			ColumnName:  "any",
		},
	}
	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		src,
		&model.MockDestination{
			SinkerFactory: validator.New(
				model.IsStrictSource(src),
				validator.InitDone(t),
				validator.Referencer(t),
				validator.TypesystemChecker(s3.ProviderType, func(colSchema abstract.ColSchema) string {
					return colSchema.OriginalType
				}),
			),
			Cleanup: model.Drop,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(1 * time.Second)
}

var processed []abstract.ChangeItem

func TestNativeS3WithProvidedSchemaAndSystemCols(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	processed = make([]abstract.ChangeItem, 0)
	testCasePath := "test_csv_all_types"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "test"
	src.TableName = "types"
	src.Format.CSVSetting = new(s3.CSVSetting)
	src.Format.CSVSetting.QuoteChar = "\""
	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024

	src.HideSystemCols = false
	src.OutputSchema = []abstract.ColSchema{
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "0",
			DataType:    schema.TypeBoolean.String(),
			ColumnName:  "boolean",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "1",
			DataType:    schema.TypeUint8.String(),
			ColumnName:  "uint8",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "2",
			DataType:    schema.TypeUint16.String(),
			ColumnName:  "uint16",
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, src, &model.MockDestination{
		SinkerFactory: validator.New(
			model.IsStrictSource(src),
			validator.Canonizator(t, storeItems),
		),
		Cleanup: model.DisabledCleanup,
	}, abstract.TransferTypeSnapshotOnly)

	helpers.Activate(t, transfer)

	require.Len(t, processed, 3)

	sampleColumns := processed[0].ColumnNames
	require.Len(t, sampleColumns, 5) // contains system columns appended at the end
	require.Equal(t, "__file_name", sampleColumns[0])
	require.Equal(t, "__row_index", sampleColumns[1])
}

func storeItems(item []abstract.ChangeItem) []abstract.ChangeItem {
	processed = append(processed, item...)
	return item
}

func TestNativeS3MissingColumnsAreFilled(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	processed = make([]abstract.ChangeItem, 0)
	testCasePath := "test_csv_all_types"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data4"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "test"
	src.TableName = "types"
	src.Format.CSVSetting = new(s3.CSVSetting)

	src.InputFormat = model.ParsingFormatCSV
	src.WithDefaults()
	src.Format.CSVSetting.BlockSize = 1 * 1024 * 1024
	src.Format.CSVSetting.QuoteChar = "\""
	src.Format.CSVSetting.AdditionalReaderOptions.IncludeMissingColumns = true
	src.HideSystemCols = true
	src.OutputSchema = []abstract.ColSchema{
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "0",
			DataType:    schema.TypeBoolean.String(),
			ColumnName:  "boolean",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "1",
			DataType:    schema.TypeUint8.String(),
			ColumnName:  "uint8",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "20",
			DataType:    schema.TypeString.String(),
			ColumnName:  "test_missing_column_string",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "21",
			DataType:    schema.TypeInt8.String(),
			ColumnName:  "test_missing_column_int",
		},
		{
			TableSchema: src.TableNamespace,
			TableName:   src.TableName,
			Path:        "22",
			DataType:    schema.TypeBoolean.String(),
			ColumnName:  "test_missing_column_bool",
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, src, &model.MockDestination{
		SinkerFactory: validator.New(
			model.IsStrictSource(src),
			validator.Canonizator(t, storeItems),
		),
		Cleanup: model.DisabledCleanup,
	}, abstract.TransferTypeSnapshotOnly)

	helpers.Activate(t, transfer)

	require.Len(t, processed, 3)

	sampleColumnValues := processed[0].ColumnValues
	require.Len(t, sampleColumnValues, 5) // contains system columns appended at the end
	require.Equal(t, "", sampleColumnValues[2])
	require.Equal(t, int8(0), sampleColumnValues[3])
	require.Equal(t, false, sampleColumnValues[4])
}
