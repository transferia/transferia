package json

import (
	"context"
	encoding_json "encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/nop"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var jsonSchemaFormats = []struct {
	name        string
	restColumn  string
	newResolver s3_reader.NewSchemaResolver
	newReader   s3_reader.NewS3Reader
}{
	{name: "json", restColumn: "_rest", newResolver: NewJSONParserSchemaResolver, newReader: NewJSONParserReader},
	{name: "jsonline", restColumn: RestColumnName, newResolver: NewJSONLineSchemaResolver, newReader: NewJSONLineReader},
}

func newJSONSchemaFixture(t *testing.T) (*s3_model.S3Source, *fake_s3.FakeS3Client, *s3raw.FakeS3RawReaderBuilder) {
	t.Helper()
	client := fake_s3.NewFakeS3Client(t)
	settings := new(s3_model.JSONLSetting)
	settings.BlockSize = 1024
	source := new(s3_model.S3Source)
	source.Bucket = "bucket"
	source.Format.JSONLSetting = settings
	return source, client, s3raw.NewFakeS3RawReaderBuilder(client, client)
}

func TestJSONSchemaMatchesRows(t *testing.T) {
	for _, format := range jsonSchemaFormats {
		for _, testCase := range []struct {
			name       string
			preset     bool
			primaryKey bool
			sample     []byte
		}{
			{name: "preset", preset: true, primaryKey: true, sample: nil},
			{name: "preset_without_key", preset: true, primaryKey: false, sample: nil},
			{name: "empty_file", preset: false, primaryKey: false, sample: nil},
			{name: "empty_object", preset: false, primaryKey: false, sample: []byte("{}\n")},
			{name: "null", preset: false, primaryKey: false, sample: []byte("null\n")},
		} {
			for _, hideSystemCols := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/hide=%t", format.name, testCase.name, hideSystemCols), func(t *testing.T) {
					ctx := context.Background()
					source, client, builder := newJSONSchemaFixture(t)
					if !testCase.preset {
						client.AddFile(fake_s3.NewFile("empty.json", testCase.sample, 1))
						client.AddFile(fake_s3.NewFile("rows.json", []byte("{\"id\":1}\n"), 2))
					}
					metrics := stats.NewSourceStats(nop.Registry{})
					source.Format.JSONLSetting.UnexpectedFieldBehavior = s3_model.Ignore
					source.TableName = "rows"
					source.ReadBatchSize = 10
					source.HideSystemCols = hideSystemCols
					if testCase.preset {
						source.OutputSchema = []abstract.ColSchema{abstract.NewColSchema("id", ytschema.TypeInt64, testCase.primaryKey)}
					}
					resolver, err := format.newResolver(source, logger.Log, nil, metrics, builder)
					require.NoError(t, err)
					// The preset must work before any files are available in S3.
					tableSchema, readerError := s3_reader.ExecuteSchemaResolver(ctx, resolver)
					require.NoError(t, readerError)
					expectedNames := []string{"id"}
					if !hideSystemCols {
						expectedNames = []string{s3_reader.FileNameSystemCol, s3_reader.RowIndexSystemCol, "id"}
					}
					require.Equal(t, expectedNames, tableSchema.Columns().ColumnNames())
					if !hideSystemCols {
						require.Equal(t, ytschema.TypeString.String(), tableSchema.Columns()[0].DataType)
						require.Equal(t, ytschema.TypeUint64.String(), tableSchema.Columns()[1].DataType)
						require.Equal(t, !testCase.primaryKey, tableSchema.Columns()[0].PrimaryKey)
						require.Equal(t, !testCase.primaryKey, tableSchema.Columns()[1].PrimaryKey)
					}
					require.Equal(t, testCase.primaryKey, tableSchema.Columns()[len(expectedNames)-1].PrimaryKey)
					if testCase.preset {
						client.AddFile(fake_s3.NewFile("rows.json", []byte("{\"id\":1}\n"), 2))
					}
					reader, err := format.newReader(source, logger.Log, nil, metrics, builder)
					require.NoError(t, err)
					var rows []abstract.ChangeItem
					chunkPusher := new(mockChunkPusher)
					chunkPusher.pushFunc = func(_ context.Context, chunk pusher.Chunk) error {
						rows = append(rows, chunk.Items...)
						return nil
					}
					require.NoError(t, reader.Read(ctx, tableSchema, "rows.json", chunkPusher))
					require.Len(t, rows, 1)
					require.Equal(t, expectedNames, rows[0].ColumnNames)
					idJSON, err := encoding_json.Marshal(rows[0].ColumnValues[len(expectedNames)-1])
					require.NoError(t, err)
					require.Equal(t, "1", string(idJSON))
					if !hideSystemCols {
						require.Equal(t, "rows.json", rows[0].ColumnValues[0])
						require.Equal(t, uint64(1), rows[0].ColumnValues[1])
					}
				})
			}
		}
	}
}

func TestJSONSchemaInferenceRejectsSystemColumns(t *testing.T) {
	for _, format := range jsonSchemaFormats {
		for _, testCase := range []struct {
			name   string
			sample string
			column string
		}{
			{name: "file_name", sample: `{"__file_name":1}`, column: s3_reader.FileNameSystemCol},
			{name: "row_index", sample: `{"__row_index":2}`, column: s3_reader.RowIndexSystemCol},
			{name: "both", sample: `{"__file_name":1,"__row_index":2,"id":3}`, column: s3_reader.FileNameSystemCol},
			{name: "file_name_after_user_field", sample: `{"!first":1,"__file_name":2,"id":3}`, column: s3_reader.FileNameSystemCol},
			{name: "row_index_after_user_field", sample: `{"!first":1,"__row_index":2,"id":3}`, column: s3_reader.RowIndexSystemCol},
			{name: "file_name_null", sample: `{"__file_name":null,"id":3}`, column: s3_reader.FileNameSystemCol},
		} {
			for _, behavior := range []s3_model.UnexpectedFieldBehavior{s3_model.Ignore, s3_model.Infer} {
				for _, hideSystemCols := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/behavior=%d/hide=%t", format.name, testCase.name, behavior, hideSystemCols), func(t *testing.T) {
						source, client, builder := newJSONSchemaFixture(t)
						client.AddFile(fake_s3.NewFile("sample.json", []byte(testCase.sample+"\n"), 1))
						source.Format.JSONLSetting.UnexpectedFieldBehavior = behavior
						source.HideSystemCols = hideSystemCols
						resolver, err := format.newResolver(source, logger.Log, nil, nil, builder)
						require.NoError(t, err)
						tableSchema, readerError := resolver.TryInferSchemaFromObject(context.Background(), "sample.json")
						require.Error(t, readerError)
						require.Nil(t, tableSchema)
						dataError, ok := reader_error.AsReaderErrorData(readerError)
						require.True(t, ok, "reserved source fields must be reported as schema errors")
						require.Equal(t, reader_error.ScopeSchema, dataError.Scope)
						require.Equal(t, "sample.json", dataError.File)
						require.ErrorContains(t, readerError, testCase.column)
						require.ErrorContains(t, readerError, "reserved")
					})
				}
			}
		}
	}
}

func TestJSONSchemaInferenceRejectsEmptySamples(t *testing.T) {
	for _, format := range jsonSchemaFormats {
		for _, testCase := range []struct {
			name   string
			sample []byte
		}{
			{name: "empty_file", sample: nil},
			{name: "empty_object", sample: []byte("{}\n")},
			{name: "null", sample: []byte("null\n")},
		} {
			for _, hideSystemCols := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/hide=%t", format.name, testCase.name, hideSystemCols), func(t *testing.T) {
					source, client, builder := newJSONSchemaFixture(t)
					client.AddFile(fake_s3.NewFile("sample.json", testCase.sample, 1))
					source.Format.JSONLSetting.UnexpectedFieldBehavior = s3_model.Ignore
					source.HideSystemCols = hideSystemCols
					resolver, err := format.newResolver(source, logger.Log, nil, nil, builder)
					require.NoError(t, err)
					tableSchema, readerError := s3_reader.ExecuteSchemaResolver(context.Background(), resolver)
					require.Error(t, readerError)
					require.Nil(t, tableSchema)
					dataError, ok := reader_error.AsReaderErrorData(readerError)
					require.True(t, ok, "empty samples must not produce a schema containing only system columns")
					require.Equal(t, reader_error.ScopeSchema, dataError.Scope)
				})
			}
		}
	}
}

func TestJSONSchemaInferencePreservesRest(t *testing.T) {
	for _, format := range jsonSchemaFormats {
		for _, sample := range []string{"{}\n", "null\n"} {
			for _, hideSystemCols := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/sample=%s/hide=%t", format.name, sample[:len(sample)-1], hideSystemCols), func(t *testing.T) {
					source, client, builder := newJSONSchemaFixture(t)
					client.AddFile(fake_s3.NewFile("sample.json", []byte(sample), 1))
					source.Format.JSONLSetting.UnexpectedFieldBehavior = s3_model.Infer
					source.HideSystemCols = hideSystemCols
					resolver, err := format.newResolver(source, logger.Log, nil, nil, builder)
					require.NoError(t, err)
					tableSchema, readerError := s3_reader.ExecuteSchemaResolver(context.Background(), resolver)
					require.NoError(t, readerError)
					expectedNames := []string{format.restColumn}
					if !hideSystemCols {
						expectedNames = []string{s3_reader.FileNameSystemCol, s3_reader.RowIndexSystemCol, format.restColumn}
					}
					require.Equal(t, expectedNames, tableSchema.Columns().ColumnNames())
					require.Equal(t, ytschema.TypeAny.String(), tableSchema.Columns()[len(expectedNames)-1].DataType)
				})
			}
		}
	}
}
