package reader

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestResolveJSONLineSchema(t *testing.T) {
	src := s3.PrepareCfg(t, "data3", model.ParsingFormatJSONLine)

	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.PathPrefix = "test_jsonline_schemas"
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

	jsonlineReader := JSONLineReader{
		client:     aws_s3.New(sess),
		logger:     logger.Log,
		pathPrefix: "test_jsonline_schemas",
		batchSize:  1 * 1024 * 1024,
		bucket:     src.Bucket,
		blockSize:  1 * 1024 * 1024,
		metrics:    stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	res, err := jsonlineReader.ResolveSchema(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("simple schema", func(t *testing.T) {
		currSchema, err := jsonlineReader.resolveSchema(context.Background(), "test_jsonline_schemas/simple.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Browser", "Cookie_Enabled", "Date", "Gender", "Hit_ID", "Region_ID", "Technology", "Time_Spent", "Traffic_Source"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "boolean", "timestamp", "utf8", "double", "double", "utf8", "utf8", "utf8"}, dataTypes(currSchema.Columns()))
	})

	t.Run("array schema", func(t *testing.T) {
		currSchema, err := jsonlineReader.resolveSchema(context.Background(), "test_jsonline_schemas/array.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Date", "Hit_ID", "Time_Spent"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"timestamp", "double", "any"}, dataTypes(currSchema.Columns()))
	})

	t.Run("object schema", func(t *testing.T) {
		currSchema, err := jsonlineReader.resolveSchema(context.Background(), "test_jsonline_schemas/object.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Date", "Hit_ID", "Time_Spent"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"timestamp", "double", "any"}, dataTypes(currSchema.Columns()))
	})

	t.Run("invalid schema", func(t *testing.T) {
		_, err := jsonlineReader.resolveSchema(context.Background(), "test_jsonline_schemas/invalid.jsonl")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to validate json line")
	})

	jsonlineReader.newlinesInValue = true

	t.Run("newline in value", func(t *testing.T) {
		currSchema, err := jsonlineReader.resolveSchema(context.Background(), "test_jsonline_schemas/newline.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Cookie_Enabled", "Date", "Gender", "Hit_ID", "Region_ID", "Technology", "Time_Spent"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"boolean", "timestamp", "any", "double", "double", "utf8", "any"}, dataTypes(currSchema.Columns()))
	})
}

func dataTypes(columns abstract.TableColumns) []string {
	result := make([]string, len(columns))
	for i, column := range columns {
		result[i] = column.DataType
	}
	return result
}

func TestTypes(t *testing.T) {
	type testStruct struct {
		Boolean bool
		String  string
		Integer int64
		Uint    uint64
		Float   float64
		Array   []interface{}
		Object  map[string]interface{}
		Date    string
	}
	testObject := testStruct{
		Boolean: true,
		String:  "something",
		Integer: -125,
		Uint:    665,
		Float:   3.8,
		Array:   []interface{}{"test", "test-2"},
		Object:  map[string]interface{}{"test": "something"},
		Date:    "2022-02-01",
	}

	jsonString, _ := json.Marshal(testObject)
	testMap := make(map[string]interface{})

	require.NoError(t, json.Unmarshal(jsonString, &testMap))
	mappedType, original, _ := guessType(testMap["Boolean"])
	require.Equal(t, schema.TypeBoolean, mappedType)
	require.Equal(t, "boolean", original)

	mappedType, original, _ = guessType(testMap["String"])
	require.Equal(t, schema.TypeString, mappedType)
	require.Equal(t, "string", original)

	mappedType, original, _ = guessType(testMap["Integer"])
	require.Equal(t, schema.TypeFloat64, mappedType)
	require.Equal(t, "number", original)

	mappedType, original, _ = guessType(testMap["Uint"])
	require.Equal(t, schema.TypeFloat64, mappedType)
	require.Equal(t, "number", original)

	mappedType, original, _ = guessType(testMap["Float"])
	require.Equal(t, schema.TypeFloat64, mappedType)
	require.Equal(t, "number", original)

	mappedType, original, _ = guessType(testMap["Date"])
	require.Equal(t, schema.TypeTimestamp, mappedType)
	require.Equal(t, "timestamp", original)

	mappedType, original, _ = guessType(testMap["Array"])
	require.Equal(t, schema.TypeAny, mappedType)
	require.Equal(t, "array", original)

	mappedType, original, _ = guessType(testMap["Object"])
	require.Equal(t, schema.TypeAny, mappedType)
	require.Equal(t, "object", original)
}
