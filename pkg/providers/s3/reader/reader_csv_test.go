package reader

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestResolveCSVSchema(t *testing.T) {
	src := s3.PrepareCfg(t, "data4", "")

	if os.Getenv("S3MDS_PORT") != "" {
		// for local recipe we need to upload test case to internet
		src.PathPrefix = "test_csv_schemas"
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey, string(src.ConnectionConfig.SecretKey), "",
		),
	})

	require.NoError(t, err)

	csvReader := CSVReader{
		client:          aws_s3.New(sess),
		pathPrefix:      "test_csv_schemas",
		maxBatchSize:    128,
		blockSize:       1 * 1024 * 1024,
		logger:          logger.Log,
		bucket:          src.Bucket,
		delimiter:       ',',
		quoteChar:       '"',
		doubleQuote:     true,
		newlinesInValue: true,
		escapeChar:      '\\',
		metrics:         stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	res, err := csvReader.ResolveSchema(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("preexisting table schema", func(t *testing.T) {
		csvReader.tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
			{
				TableSchema: "test-schema",
				TableName:   "test-name",
				ColumnName:  "test-1",
				PrimaryKey:  false,
			}, {
				TableSchema: "test-schema",
				TableName:   "test-name",
				ColumnName:  "test-2",
				PrimaryKey:  true,
			},
		})

		expectedSchema, err := csvReader.ResolveSchema(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, len(expectedSchema.Columns()))
		require.Equal(t, csvReader.tableSchema, expectedSchema)
	})

	t.Run("first line header schema", func(t *testing.T) {
		currSchema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/simple.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(currSchema.Columns()))
	})

	t.Run("autogenerate schema", func(t *testing.T) {
		csvReader.advancedOptions.AutogenerateColumnNames = true
		currSchema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"f0", "f1", "f2", "f3", "f4", "f5"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(currSchema.Columns()))
	})

	t.Run("extract schema", func(t *testing.T) {
		csvReader.advancedOptions.ColumnNames = []string{"name", "surname", "st.", "city", "state", "zip-code"}
		currSchema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(currSchema.Columns()))
	})
}

func TestConstructCI(t *testing.T) {
	csvReader := CSVReader{
		logger:  logger.Log,
		metrics: stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	csvReader.tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableSchema: "test-schema",
			TableName:   "test-name",
			ColumnName:  "test-first-column",
			DataType:    schema.TypeBoolean.String(),
			PrimaryKey:  false,
			Path:        "0",
		},
		{
			TableSchema: "test-schema",
			TableName:   "test-name",
			ColumnName:  "test-missing-row-column",
			DataType:    schema.TypeString.String(),
			PrimaryKey:  false,
			Path:        "1",
		},
	})

	t.Run("missing cols are included", func(t *testing.T) {
		row := []string{"true"} // only one element in row from csv but 2 cols in schema
		csvReader.additionalReaderOptions.IncludeMissingColumns = true
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 2)
		require.Equal(t, []interface{}{true, ""}, ci.ColumnValues)
	})

	t.Run("missing cols flag is disabled", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true"} // only one element in row from csv
		_, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.Error(t, err)
		require.ErrorContains(t, err, "missing row element for column: test-missing-row-column, row elements: 1, columns: 2")
	})

	t.Run("missing cols flag is disabled but all elements present", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 2 cols
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 2)
		require.Equal(t, []interface{}{true, "this is a test string"}, ci.ColumnValues)
	})

	t.Run("schema contains sys cols", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		csvReader.tableSchema = appendSystemColsTableSchema(csvReader.tableSchema.Columns())
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 4 cols, but 2 are sys cols
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 4) // we expect 4 values 2 that we read and 32 from the sys cols
		require.Equal(t, []interface{}{"test_file", uint64(1), true, "this is a test string"}, ci.ColumnValues)
	})
}
