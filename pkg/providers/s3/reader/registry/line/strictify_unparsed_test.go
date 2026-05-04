package line

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	parent_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// TestLineReaderStrictifyContinueSurfacesUnparsed change from base.md: strictify error is not silently dropped.
func TestLineReaderStrictifyContinueSurfacesUnparsed(t *testing.T) {
	key := "l/badline.txt"
	body := []byte("not-a-uint64-value-for-line\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	builder := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()

	src := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "l",
		InputFormat:      model.ParsingFormatLine,
		UnparsedPolicy:   s3_model.UnparsedPolicyContinue,
		HideSystemCols:   true,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	ucol := abstract.NewColSchema("row", ytschema.TypeUint64, false)
	src.OutputSchema = []abstract.ColSchema{ucol}
	src.WithDefaults()

	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewLineReader(src, logger.Log, sess, st, builder)
	require.NoError(t, err)
	schema, err := NewLineSchemaResolver(src, logger.Log, sess, st, builder)
	require.NoError(t, err)
	r := parent_reader.NewReaderContractor(read, schema)
	var out []abstract.ChangeItem
	p := &collectingPusher{out: &out}
	err = r.Read(context.Background(), key, p)
	require.NoError(t, err)
	require.NotEmpty(t, out)
	var found bool
	if slices.ContainsFunc(out, parsers.IsUnparsed) {
		found = true
	}
	require.True(t, found, "expected at least one *_unparsed item for strictify failure with Continue")
}

// TestLineReaderStrictifyFailAndRetry: strictify parse failure is fatal for Fail, non-fatal for Retry.
func TestLineReaderStrictifyFailAndRetry(t *testing.T) {
	key := "l/badline_policy.txt"
	body := []byte("not-a-uint64-value-for-line\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	builder := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()

	ucol := abstract.NewColSchema("row", ytschema.TypeUint64, false)
	base := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "l",
		InputFormat:      model.ParsingFormatLine,
		HideSystemCols:   true,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	base.OutputSchema = []abstract.ColSchema{ucol}

	base.UnparsedPolicy = s3_model.UnparsedPolicyFail
	base.WithDefaults()
	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewLineReader(base, logger.Log, sess, st, builder)
	require.NoError(t, err)
	schema, err := NewLineSchemaResolver(base, logger.Log, sess, st, builder)
	require.NoError(t, err)
	r := parent_reader.NewReaderContractor(read, schema)
	err = r.Read(context.Background(), key, &collectingPusher{out: new([]abstract.ChangeItem)})
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	base = &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "l",
		InputFormat:      model.ParsingFormatLine,
		HideSystemCols:   true,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
		OutputSchema:     []abstract.ColSchema{ucol},
		UnparsedPolicy:   s3_model.UnparsedPolicyRetry,
	}
	base.WithDefaults()
	read, err = NewLineReader(base, logger.Log, sess, st, builder)
	require.NoError(t, err)
	schema, err = NewLineSchemaResolver(base, logger.Log, sess, st, builder)
	require.NoError(t, err)
	r = parent_reader.NewReaderContractor(read, schema)
	err = r.Read(context.Background(), key, &collectingPusher{out: new([]abstract.ChangeItem)})
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
	_, ok := s3_reader.AsReaderErrorData(err)
	require.True(t, ok, "expected data error, got: %v", err)
}

type collectingPusher struct {
	out *[]abstract.ChangeItem
}

func (c *collectingPusher) IsEmpty() bool                     { return true }
func (c *collectingPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (c *collectingPusher) Push(_ context.Context, ch s3_pusher.Chunk) error {
	*c.out = append(*c.out, ch.Items...)
	return nil
}
