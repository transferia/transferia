package snapshotjsonline

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	canon_reference "github.com/transferia/transferia/tests/canon/reference"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	//go:embed testdata/dynamo.jsonl
	content []byte
	fname   = "dynamo.jsonl"
)

func buildSourceModel(t *testing.T) *s3_model.S3Source {
	src := s3recipe.PrepareCfg(t, "", "")
	src.TableNamespace = "example"
	src.TableName = "data"
	src.InputFormat = model.ParsingFormatJSON
	src.Format.JSONLSetting = new(s3_model.JSONLSetting)
	src.Format.JSONLSetting.BlockSize = 1 * 1024 * 1024
	src.OutputSchema = []abstract.ColSchema{
		{ColumnName: "OrderID", DataType: ytschema.TypeString.String(), Path: "Item.OrderID.S", PrimaryKey: true},
		{ColumnName: "OrderDate", DataType: ytschema.TypeDatetime.String(), Path: "Item.OrderDate.S"},
		{ColumnName: "CustomerName", DataType: ytschema.TypeString.String(), Path: "Item.CustomerName.S"},
		{ColumnName: "CustomerAmount", DataType: ytschema.TypeInt32.String(), Path: "Item.OrderAmount.N"},
	}
	src.WithDefaults()
	return src
}

func testNativeS3(t *testing.T, src *s3_model.S3Source) {
	dst := *chrecipe.MustTarget(chrecipe.WithInitFile("initdb.sql"), chrecipe.WithDatabase("example"))

	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey, string(src.ConnectionConfig.SecretKey), "",
		),
	})
	require.NoError(t, err)

	uploader := s3manager.NewUploader(sess)
	buff := bytes.NewReader(content)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   buff,
		Bucket: aws.String(src.Bucket),
		Key:    aws.String(fname),
	})
	require.NoError(t, err)

	dst.WithDefaults()
	transfer := transferhelpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &dst, "example", "data", 2)

	canon_reference.Dump(t, &clickhouse_model.ChSource{
		Database:   "example",
		ShardsList: []clickhouse_model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		NativePort: dst.NativePort,
		HTTPPort:   dst.HTTPPort,
		User:       dst.User,
	})
}

func TestAll(t *testing.T) {
	src := buildSourceModel(t)
	testNativeS3(t, src)
	helpers.TestS3SchemaAndPkeyCases(t, src, "OrderID", "Item.OrderID.S")
}
