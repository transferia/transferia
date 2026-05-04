package json

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestResolveJSONLineSchema(t *testing.T) {
	src := s3recipe.PrepareCfg(t, "data3", model.ParsingFormatJSONLine)

	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.PathPrefix = "test_jsonline_schemas"
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

	metrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	resolverIface, err := NewJSONLineSchemaResolver(src, logger.Log, sess, metrics, s3raw.NewRealS3RawReaderBuilder())
	require.NoError(t, err)
	resolver, ok := resolverIface.(*JSONLineSchemaResolver)
	require.True(t, ok)

	res, err := reader.ExecuteSchemaResolver(context.Background(), resolver)
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("simple schema", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/simple.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Browser", "Cookie_Enabled", "Date", "Gender", "Hit_ID", "Region_ID", "Technology", "Time_Spent", "Traffic_Source", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "boolean", "timestamp", "utf8", "double", "double", "utf8", "utf8", "utf8", "any"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("array schema", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/array.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Date", "Hit_ID", "Time_Spent", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"timestamp", "double", "any", "any"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("object schema", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/object.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Date", "Hit_ID", "Time_Spent", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"timestamp", "double", "any", "any"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("invalid schema", func(t *testing.T) {
		_, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/invalid.jsonl")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to validate json line")
	})

	t.Run("valid json without newline", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/without_new_line_one_json.json")
		require.NoError(t, err)
		require.Equal(t, []string{"Item", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"any", "any"}, reader.DataTypes(currSchema.Columns()))
	})

	resolver.newlinesInValue = true

	t.Run("newline in value", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/newline.jsonl")
		require.NoError(t, err)
		require.Equal(t, []string{"Cookie_Enabled", "Date", "Gender", "Hit_ID", "Region_ID", "Technology", "Time_Spent", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"boolean", "timestamp", "any", "double", "double", "utf8", "any", "any"}, reader.DataTypes(currSchema.Columns()))
	})

	t.Run("valid json without newline one json", func(t *testing.T) {
		currSchema, err := resolver.resolveSchema(context.Background(), "test_jsonline_schemas/without_new_line_one_json.json")
		require.NoError(t, err)
		require.Equal(t, []string{"Item", "rest"}, currSchema.Columns().ColumnNames())
		require.Equal(t, []string{"any", "any"}, reader.DataTypes(currSchema.Columns()))
	})
}

func TestTypes(t *testing.T) {
	type testStruct struct {
		Boolean bool
		String  string
		Integer int64
		Uint    uint64
		Float   float64
		Array   []any
		Object  map[string]any
		Date    string
	}
	testObject := testStruct{
		Boolean: true,
		String:  "something",
		Integer: -125,
		Uint:    665,
		Float:   3.8,
		Array:   []any{"test", "test-2"},
		Object:  map[string]any{"test": "something"},
		Date:    "2022-02-01",
	}

	jsonString, _ := json.Marshal(testObject)
	testMap := make(map[string]any)

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
