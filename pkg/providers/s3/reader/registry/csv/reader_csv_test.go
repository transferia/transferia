package csv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestResolveCSVSchema(t *testing.T) {
	src := s3recipe.PrepareCfg(t, "data4", "")

	if os.Getenv("S3MDS_PORT") != "" {
		// for local recipe we need to upload test case to internet
		src.PathPrefix = "test_csv_schemas"
		s3recipe.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}

	sess, err := session.NewSession(
		&aws.Config{
			Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
			Region:           aws.String(src.ConnectionConfig.Region),
			S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
			Credentials: credentials.NewStaticCredentials(
				src.ConnectionConfig.AccessKey,
				string(src.ConnectionConfig.SecretKey),
				"",
			),
		},
	)

	require.NoError(t, err)

	infra := csvConfig{
		bucket: src.Bucket, client: aws_s3.New(sess),

		logger: logger.Log, blockSize: 1 * 1024 * 1024, pathPrefix: "test_csv_schemas",

		delimiter: ',',
		quoteChar: '"',

		escapeChar: '\\', doubleQuote: true,
		newlinesInValue: true,

		metrics: stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	resolver := &CSVSchemaResolver{config: infra, tableSchema: nil}

	res, err := reader.ExecuteSchemaResolver(context.Background(), resolver)
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("preexisting table schema", func(t *testing.T) {
		resolver.tableSchema = abstract.NewTableSchema(
			[]abstract.ColSchema{
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
			},
		)

		expectedSchema, err := reader.ExecuteSchemaResolver(context.Background(), resolver)
		require.NoError(t, err)
		require.Equal(t, 2, len(expectedSchema.Columns()))
		require.Equal(t, resolver.tableSchema, expectedSchema)
	})

	t.Run("first line header schema", func(t *testing.T) {
		currSchema, err := csvInferTableSchemaFromSample(context.Background(), &resolver.config, "test_csv_schemas/simple.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("autogenerate schema", func(t *testing.T) {
		resolver.config.advancedOptions.AutogenerateColumnNames = true
		currSchema, err := csvInferTableSchemaFromSample(context.Background(), &resolver.config, "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"f0", "f1", "f2", "f3", "f4", "f5"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("extract schema", func(t *testing.T) {
		resolver.config.advancedOptions.ColumnNames = []string{"name", "surname", "st.", "city", "state", "zip-code"}
		currSchema, err := csvInferTableSchemaFromSample(context.Background(), &resolver.config, "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, reader.DataTypes(currSchema.Columns()))
	})
}

func TestEstimateRows_NoCompleteLinesReturnsZero(t *testing.T) {
	src := s3recipe.PrepareCfg(t, "estimate_rows", "")

	key := "estimate_rows/no_newline.csv"
	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0o755))
	content := []byte("col1,col2")
	require.NoError(t, os.WriteFile(localPath, content, 0o644))

	s3recipe.UploadOne(t, src, localPath)

	sess, err := session.NewSession(
		&aws.Config{
			Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
			Region:           aws.String(src.ConnectionConfig.Region),
			S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
			Credentials: credentials.NewStaticCredentials(
				src.ConnectionConfig.AccessKey,
				string(src.ConnectionConfig.SecretKey),
				"",
			),
		},
	)
	require.NoError(t, err)

	r := &CSVReader{
		config: csvConfig{
			bucket: src.Bucket, client: aws_s3.New(sess),

			logger: logger.Log, blockSize: 1 * 1024, pathPrefix: "estimate_rows",

			delimiter: ',',
			quoteChar: '"',

			escapeChar: '\\', doubleQuote: true,
			newlinesInValue: true,

			metrics: stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		},
		table:          abstract.TableID{Namespace: "ns", Name: "t"},
		unparsedPolicy: s3_model.UnparsedPolicyContinue,
		maxBatchSize:   128,
	}

	rows, err := r.EstimateRowsCountAllObjects(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), rows)
}

func TestConstructCI(t *testing.T) {
	csvReader := CSVReader{
		config: csvConfig{
			logger:  logger.Log,
			metrics: stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		},
		table: abstract.TableID{
			Namespace: "test-schema",
			Name:      "test-name",
		},
		unparsedPolicy: s3_model.UnparsedPolicyContinue,
		maxBatchSize:   128,
	}

	baseSchema := abstract.NewTableSchema(
		[]abstract.ColSchema{
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
		},
	)
	colNames := yslices.Map(baseSchema.Columns(), func(c abstract.ColSchema) string { return c.ColumnName })

	t.Run("missing cols are included", func(t *testing.T) {
		row := []string{"true"} // only one element in row from csv but 2 cols in schema
		csvReader.config.additionalReaderOptions.IncludeMissingColumns = true
		changeItem, err := csvReader.constructCI(row, "test_file", time.Now(), 1, baseSchema, colNames)
		require.NoError(t, err)
		require.Len(t, changeItem.ColumnValues, 2)
		require.Equal(t, []any{true, ""}, changeItem.ColumnValues)
	})

	t.Run("missing cols flag is disabled", func(t *testing.T) {
		csvReader.config.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true"} // only one element in row from csv
		_, err := csvReader.constructCI(row, "test_file", time.Now(), 1, baseSchema, colNames)
		require.Error(t, err)
		require.ErrorContains(t, err, "missing row element for column: test-missing-row-column, row elements: 1, columns: 2")
	})

	t.Run("missing cols flag is disabled but all elements present", func(t *testing.T) {
		csvReader.config.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 2 cols
		changeItem, err := csvReader.constructCI(row, "test_file", time.Now(), 1, baseSchema, colNames)
		require.NoError(t, err)
		require.Len(t, changeItem.ColumnValues, 2)
		require.Equal(t, []any{true, "this is a test string"}, changeItem.ColumnValues)
	})

	t.Run("schema contains sys cols", func(t *testing.T) {
		csvReader.config.additionalReaderOptions.IncludeMissingColumns = false
		sysSchema := reader.AppendSystemColsTableSchema(baseSchema.Columns(), true)
		sysColNames := yslices.Map(sysSchema.Columns(), func(c abstract.ColSchema) string { return c.ColumnName })
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 4 cols, but 2 are sys cols
		changeItem, err := csvReader.constructCI(row, "test_file", time.Now(), 1, sysSchema, sysColNames)
		require.NoError(t, err)
		require.Len(t, changeItem.ColumnValues, 4) // we expect 4 values 2 that we read and 32 from the sys cols
		require.Equal(t, []any{"test_file", uint64(1), true, "this is a test string"}, changeItem.ColumnValues)
	})

	t.Run("hide sys cols", func(t *testing.T) {
		csvReader.config.additionalReaderOptions.IncludeMissingColumns = false
		csvReader.config.hideSystemCols = true
		userCols := abstract.NewTableSchema(
			[]abstract.ColSchema{
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
			},
		)
		sysSchema := reader.AppendSystemColsTableSchema(userCols.Columns(), true)
		sysColNames := yslices.Map(sysSchema.Columns(), func(c abstract.ColSchema) string { return c.ColumnName })
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 4 cols, but 2 are sys cols
		changeItem, err := csvReader.constructCI(row, "test_file", time.Now(), 1, sysSchema, sysColNames)
		require.NoError(t, err)
		require.Len(t, changeItem.ColumnValues, 4) // we expect 4 values 2 that we read and 32 from the sys cols
		require.Equal(t, []any{nil, nil, true, "this is a test string"}, changeItem.ColumnValues)
	})
}

func TestParseFloatValue(t *testing.T) {
	r := &CSVReader{config: csvConfig{additionalReaderOptions: s3_model.AdditionalOptions{DecimalPoint: ","}}}

	// Test case 1: Valid float value with DecimalPoint "," original value is changed
	originalValue := "123,456"
	expected := "123.456"
	result := r.parseFloatValue(originalValue).(string)
	require.Equal(t, expected, result, "Test case 1 failed")

	// Test case 2: Valid float value with DecimalPoint "." nothing to change
	r.config.additionalReaderOptions.DecimalPoint = "."
	originalValue = "123.456"
	expected = "123.456"
	result = r.parseFloatValue(originalValue).(string)
	require.Equal(t, expected, result, "Test case 2 failed")

	// Test case 3: Invalid float value, original value is kept
	originalValue = "abc"
	expected = "abc"
	result = r.parseFloatValue(originalValue).(string)
	require.Equal(t, expected, result, "Test case 3 failed")

	// Test case 4: No DecimalPoint set original value is kept
	r.config.additionalReaderOptions.DecimalPoint = ""
	originalValue = "123.456"
	expected = "123.456"
	result = r.parseFloatValue(originalValue).(string)
	require.Equal(t, expected, result, "Test case 4 failed")
}

func TestParseNullValues(t *testing.T) {
	r := &CSVReader{
		config: csvConfig{
			additionalReaderOptions: s3_model.AdditionalOptions{
				StringsCanBeNull:       true,
				QuotedStringsCanBeNull: true,
				NullValues:             []string{"NULL", "NA"},
			},
		},
	}

	// Test case 1: Original value is a quoted string and a null value
	originalValue := "\"NULL\""
	col := abstract.ColSchema{} // empty column schema for demonstration
	expected := abstract.DefaultValue(&col)
	result := r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 1 failed")

	// Test case 2: Original value is a quoted string but not a null value
	originalValue = "\"notnull\""
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 2 failed")

	// Test case 3: Original value is not a quoted string but a null value
	originalValue = "NULL"
	expected = abstract.DefaultValue(&col)
	result = r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 3 failed")

	// Test case 4: Original value is not a quoted string and not a null value
	originalValue = "notnull"
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 4 failed")

	// Test case 5: StringsCanBeNull and QuotedStringsCanBeNull are both false
	r.config.additionalReaderOptions.StringsCanBeNull = false
	r.config.additionalReaderOptions.QuotedStringsCanBeNull = false
	originalValue = "\"NULL\""
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 5 failed")

	// Test case 6: Original value is not in the NullValues list
	r.config.additionalReaderOptions.StringsCanBeNull = true
	r.config.additionalReaderOptions.QuotedStringsCanBeNull = true
	originalValue = "notnull"
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(t, expected, result, "Test case 6 failed")
}

func TestParseDateValue(t *testing.T) {
	r := &CSVReader{
		config: csvConfig{
			additionalReaderOptions: s3_model.AdditionalOptions{
				TimestampParsers: []string{
					"2006-01-02",      // yyyy-mm-dd
					"02-Jan-2006",     // dd-Mon-yyyy
					"January 2, 2006", // Month dd, yyyy
				},
			},
		},
	}

	// Test case 1: Original value can be parsed with the first timestamp parser
	originalValue := "2024-03-22"
	expected, _ := time.Parse("2006-01-02", originalValue)
	result := r.parseDateValue(originalValue).(time.Time)
	require.Equal(t, expected, result, "Test case 1 failed")

	// Test case 2: Original value can be parsed with the second timestamp parser
	originalValue = "22-Mar-2024"
	expected, _ = time.Parse("02-Jan-2006", originalValue)
	result = r.parseDateValue(originalValue).(time.Time)
	require.Equal(t, expected, result, "Test case 2 failed")

	// Test case 3: Original value can be parsed with the third timestamp parser
	originalValue = "March 22, 2024"
	expected, _ = time.Parse("January 2, 2006", originalValue)
	result = r.parseDateValue(originalValue).(time.Time)
	require.Equal(t, expected, result, "Test case 3 failed")

	// Test case 4: Original value cannot be parsed with any timestamp parser
	originalValue = "2024/03/22"
	res := r.parseDateValue(originalValue)
	require.Equal(t, originalValue, res, "Test case 4 failed")
}

func TestParseBooleanValue(t *testing.T) {
	r := &CSVReader{
		config: csvConfig{
			additionalReaderOptions: s3_model.AdditionalOptions{
				StringsCanBeNull: true,
				NullValues:       []string{"NULL", "NA"},
				TrueValues:       []string{"true", "yes", "1"},
				FalseValues:      []string{"false", "no", "0"},
			},
		},
	}

	// Test case 1: Original value is a null value
	originalValue := "NULL"
	result := r.parseBooleanValue(originalValue).(bool)
	require.Equal(t, false, result, "Test case 1 failed")

	// Test case 2: Original value is a true value
	originalValue = "true"
	result = r.parseBooleanValue(originalValue).(bool)
	require.Equal(t, true, result, "Test case 2 failed")

	// Test case 3: Original value is a false value
	originalValue = "false"
	result = r.parseBooleanValue(originalValue).(bool)
	require.Equal(t, false, result, "Test case 3 failed")

	// Test case 4: Original value is not in any of the true/false/null values lists, but can be parsed as boolean
	originalValue = "TRUE"
	result = r.parseBooleanValue(originalValue).(bool)
	require.Equal(t, true, result, "Test case 4 failed")

	// Test case 5: Original value is not in any of the true/false/null values lists and cannot be parsed as boolean
	originalValue = "random"
	res := r.parseBooleanValue(originalValue)
	require.Equal(t, originalValue, res, "Test case 5 failed")
}
