package jsonline

import (
	_ "embed"
	"os"
	"testing"
	"time"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/tests/canon/validator"
	"github.com/transferria/transferria/tests/helpers"
)

func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	testCasePath := "test_jsonline_all_types"
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
	src.InputFormat = model.ParsingFormatJSONLine
	src.WithDefaults()
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024
	src.HideSystemCols = false

	src.OutputSchema = []abstract.ColSchema{
		{
			ColumnName:   "array",
			OriginalType: "jsonl:array",
			DataType:     "any",
		},
		{
			ColumnName:   "boolean",
			OriginalType: "jsonl:boolean",
			DataType:     "boolean",
		},
		{
			ColumnName:   "date",
			OriginalType: "jsonl:string",
			DataType:     "utf8",
		},
		{
			ColumnName:   "id",
			OriginalType: "jsonl:number",
			DataType:     "double",
		},
		{
			ColumnName:   "name",
			OriginalType: "jsonl:string",
			DataType:     "utf8",
		},
		{
			ColumnName:   "object",
			OriginalType: "jsonl:object",
			DataType:     "any",
		},
		{
			ColumnName:   "rest",
			OriginalType: "jsonl:object",
			DataType:     "any",
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
