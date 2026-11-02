package skipnonstricttables

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func writeTable(t *testing.T, ctx context.Context, client yt.Client, tablePath ypath.Path, schema ytschema.Schema, rows int, extraColumn bool) {
	t.Helper()
	writer, err := yt.WriteTable(ctx, client, tablePath, yt.WithCreateOptions(yt.WithSchema(schema), yt.WithRecursive()))
	require.NoError(t, err)
	defer func() { _ = writer.Rollback() }()
	for i := 0; i < rows; i++ {
		row := map[string]any{"key": int64(i), "value": fmt.Sprintf("%s:%d", path.Base(tablePath.String()), i)}
		if extraColumn {
			row["undeclared"] = "not described by the table schema"
		}
		require.NoError(t, writer.Write(row))
	}
	require.NoError(t, writer.Commit())
	var actualSchema ytschema.Schema
	require.NoError(t, client.GetNode(ctx, tablePath.Attr("schema"), &actualSchema, nil))
	require.Equal(t, schema.Strict, actualSchema.Strict)
	require.Len(t, actualSchema.Columns, len(schema.Columns))
}

func readBucket(t *testing.T, ctx context.Context, target *s3_model.S3Destination, skippedTableNames []string) map[string]int64 {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(target.Endpoint),
		Region:           aws.String(target.Region),
		S3ForcePathStyle: aws.Bool(target.S3ForcePathStyle),
		Credentials:      credentials.NewStaticCredentials(target.AccessKey, target.Secret, ""),
	})
	require.NoError(t, err)
	client := aws_s3.New(sess)
	rows := make(map[string]int64)
	err = client.ListObjectsPagesWithContext(ctx, &aws_s3.ListObjectsInput{Bucket: aws.String(target.Bucket)}, func(page *aws_s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range page.Contents {
			for _, skipped := range skippedTableNames {
				require.NotContains(t, *object.Key, skipped, "skipped tables must not produce S3 objects")
			}
			result, err := client.GetObjectWithContext(ctx, &aws_s3.GetObjectInput{Bucket: aws.String(target.Bucket), Key: object.Key})
			require.NoError(t, err)
			func() {
				defer func() { require.NoError(t, result.Body.Close()) }()
				decoder := json.NewDecoder(result.Body)
				decoder.DisallowUnknownFields()
				for {
					var row struct {
						Key   int64  `json:"key"`
						Value string `json:"value"`
					}
					if err := decoder.Decode(&row); err == io.EOF {
						break
					} else {
						require.NoError(t, err)
					}
					_, exists := rows[row.Value]
					require.False(t, exists, "a source row must be delivered exactly once: %s", row.Value)
					rows[row.Value] = row.Key
				}
			}()
		}
		return true
	})
	require.NoError(t, err)
	return rows
}

func TestSkipTablesWithUnsupportedSchema(t *testing.T) {
	t.Setenv("YC", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, stopEnv := recipe.NewEnv(t)
	t.Cleanup(stopEnv)
	client := env.YT
	root := ypath.Path("//home/cdc/junk/" + t.Name())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		require.NoError(t, client.RemoveNode(cleanupCtx, root, &yt.RemoveNodeOptions{Recursive: true, Force: true}))
	})
	strictSchema := ytschema.Schema{
		Strict: ptr.Bool(true),
		Columns: []ytschema.Column{
			{Name: "key", ComplexType: ytschema.TypeInt64},
			{Name: "value", ComplexType: ytschema.TypeString},
		},
	}
	// Above the snapshot loader's minimum shard size, so the successful
	// transfer exercises partition cookies and secondary workers as well.
	const bigRows = 60000
	writeTable(t, ctx, client, root.Child("big"), strictSchema, bigRows, false)
	writeTable(t, ctx, client, root.Child("small"), strictSchema, 2, false)
	nonStrictSchema := strictSchema
	nonStrictSchema.Strict = ptr.Bool(false)
	skippedTables := []struct {
		name        string
		schema      ytschema.Schema
		rows        int
		extraColumn bool
	}{
		{name: "empty_schema", schema: ytschema.Schema{Strict: ptr.Bool(false)}, rows: 1, extraColumn: true},
		{name: "non_strict", schema: nonStrictSchema, rows: 1, extraColumn: true},
		{name: "strict_empty", schema: ytschema.Schema{Strict: ptr.Bool(true)}, rows: 0, extraColumn: false},
	}
	skippedTableNames := make([]string, 0, len(skippedTables))
	for _, table := range skippedTables {
		writeTable(t, ctx, client, root.Child(table.name), table.schema, table.rows, table.extraColumn)
		skippedTableNames = append(skippedTableNames, table.name)
	}

	source := &provider_yt.YtSource{
		Cluster:              os.Getenv("YT_PROXY"),
		YtProxy:              os.Getenv("YT_PROXY"),
		YtToken:              os.Getenv("YT_TOKEN"),
		Paths:                []string{root.String()},
		DesiredPartSizeBytes: 128 * 1024,
	}
	target := s3recipe.PrepareS3(t, "tm-10763-skip", model.ParsingFormatJSON, s3_model.NoEncoding)
	transfer := transferhelpers.WithLocalRuntime(
		transferhelpers.MakeTransfer(transferhelpers.GenerateTransferID(t.Name()), source, target, abstract.TransferTypeSnapshotOnly),
		2, 1,
	)
	cp := coordinator.NewStatefulFakeClient()
	worker, err := delivery.ActivateShardedWithCP(ctx, cp, nil, transfer, testmetrics.EmptyRegistry())
	if worker != nil {
		defer worker.Close(t)
	}
	require.NoError(t, err)
	parts, err := cp.GetOperationTablesParts(transfer.ID)
	require.NoError(t, err)
	bigParts := 0
	for _, part := range parts {
		require.Contains(t, []string{"big", "small"}, part.Name, "skipped tables must not be scheduled")
		if part.Name == "big" {
			bigParts++
		}
	}
	require.Greater(t, bigParts, 1, "the fixture must exercise partitioned reads")
	rows := readBucket(t, ctx, target, skippedTableNames)
	require.Len(t, rows, bigRows+2)
	for i := 0; i < bigRows; i++ {
		key := fmt.Sprintf("big:%d", i)
		value, ok := rows[key]
		require.True(t, ok, "source row %s must be delivered", key)
		require.Equal(t, int64(i), value, "source row %s", key)
	}
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("small:%d", i)
		value, ok := rows[key]
		require.True(t, ok, "source row %s must be delivered", key)
		require.Equal(t, int64(i), value, "source row %s", key)
	}
}
