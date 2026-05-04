package json

import (
	"context"
	"testing"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	parent_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func contractJSONLineReader(t *testing.T, src *s3_model.S3Source, sess *aws_session.Session, b s3raw.S3RawReaderBuilder) parent_reader.Reader {
	t.Helper()
	read, err := NewJSONLineReader(src, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())), b)
	require.NoError(t, err)
	sch, err := NewJSONLineSchemaResolver(src, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())), b)
	require.NoError(t, err)
	return parent_reader.NewReaderContractor(read, sch)
}

// TestJSONLineEmptyObjectSampleDataPolicies: empty file cannot produce a JSONL sample for schema inference (DataSchema).
func TestJSONLineEmptyObjectSampleDataPolicies(t *testing.T) {
	key := "emptyobj.jsonl"
	// No JSON lines — schema inference cannot read a valid sample.
	body := []byte("")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()
	base := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "",
		InputFormat:      model.ParsingFormatJSONLine,
		Format:           s3_model.Format{JSONLSetting: &s3_model.JSONLSetting{BlockSize: 4096}},
		UnparsedPolicy:   s3_model.UnparsedPolicyContinue,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	base.WithDefaults()
	r := contractJSONLineReader(t, base, sess, b)
	p := &sinkItemsPusher{}
	err := r.Read(context.Background(), key, p)
	require.NoError(t, err)
	require.NotEmpty(t, p.items, "expected file-level or row unparsed for empty jsonl with Continue")
}

// TestJSONLineEmptyObject_FailRetry: empty file cannot infer schema (DataSchema) — Fail is fatal, Retry is not.
func TestJSONLineEmptyObject_FailRetry(t *testing.T) {
	key := "empty_fail.jsonl"
	body := []byte("")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()
	base := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "",
		InputFormat:      model.ParsingFormatJSONLine,
		Format:           s3_model.Format{JSONLSetting: &s3_model.JSONLSetting{BlockSize: 4096}},
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	base.WithDefaults()

	base.UnparsedPolicy = s3_model.UnparsedPolicyFail
	r := contractJSONLineReader(t, base, sess, b)
	err := r.Read(context.Background(), key, &sinkItemsPusher{})
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	base.UnparsedPolicy = s3_model.UnparsedPolicyRetry
	r = contractJSONLineReader(t, base, sess, b)
	err = r.Read(context.Background(), key, &sinkItemsPusher{})
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}

// TestJSONLineRowCoercionDataLayer: with fixed schema, a bad second line is DataRecord and respects Retry (non-fatal data error).
func TestJSONLineBadRowRespectsRetry(t *testing.T) {
	key := "rows.jsonl"
	body := []byte("{\"a\":1}\n{\"a\":\"nope\"}\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()
	col := abstract.NewColSchema("a", ytschema.TypeInt64, false)
	col.Path = "a"
	src := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		InputFormat:      model.ParsingFormatJSONLine,
		Format:           s3_model.Format{JSONLSetting: &s3_model.JSONLSetting{BlockSize: 4096}},
		OutputSchema:     []abstract.ColSchema{col},
		UnparsedPolicy:   s3_model.UnparsedPolicyRetry,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	src.WithDefaults()
	r := contractJSONLineReader(t, src, sess, b)
	p := &sinkItemsPusher{}
	err := r.Read(context.Background(), key, p)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
	_, ok := reader_error.AsReaderErrorData(err)
	require.True(t, ok, "expected data-layer error: %v", err)
}

// TestJSONLineBadRowContinueAndFail: same fixture as Retry — Continue surfaces unparsed row, Fail is fatal.
func TestJSONLineBadRowContinueAndFail(t *testing.T) {
	key := "rows_cf.jsonl"
	body := []byte("{\"a\":1}\n{\"a\":\"nope\"}\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()
	col := abstract.NewColSchema("a", ytschema.TypeInt64, false)
	col.Path = "a"
	base := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		InputFormat:      model.ParsingFormatJSONLine,
		Format:           s3_model.Format{JSONLSetting: &s3_model.JSONLSetting{BlockSize: 4096}},
		OutputSchema:     []abstract.ColSchema{col},
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	base.WithDefaults()
	base.UnparsedPolicy = s3_model.UnparsedPolicyContinue
	r := contractJSONLineReader(t, base, sess, b)
	p := &sinkItemsPusher{}
	err := r.Read(context.Background(), key, p)
	require.NoError(t, err)
	require.NotEmpty(t, p.items)

	base.UnparsedPolicy = s3_model.UnparsedPolicyFail
	r = contractJSONLineReader(t, base, sess, b)
	err = r.Read(context.Background(), key, &sinkItemsPusher{})
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
}

// TestJSONLinePusherErrorIgnoresUnparsedPolicy: sink failure is always LayerSink.
func TestJSONLinePusherErrorIgnoresUnparsedPolicy(t *testing.T) {
	key := "ok.jsonl"
	body := []byte("{\"a\":1}\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()
	col := abstract.NewColSchema("a", ytschema.TypeInt64, false)
	col.Path = "a"
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyContinue, s3_model.UnparsedPolicyFail, s3_model.UnparsedPolicyRetry,
	} {
		t.Run(string(pol), func(t *testing.T) {
			base := &s3_model.S3Source{
				TableNamespace:   "ns",
				TableName:        "t",
				Bucket:           "b",
				InputFormat:      model.ParsingFormatJSONLine,
				Format:           s3_model.Format{JSONLSetting: &s3_model.JSONLSetting{BlockSize: 4096}},
				OutputSchema:     []abstract.ColSchema{col},
				UnparsedPolicy:   pol,
				ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
			}
			base.WithDefaults()
			r := contractJSONLineReader(t, base, sess, b)
			sinkErr := xerrors.New("push failed")
			p := &pusherWithErr{err: sinkErr}
			err := r.Read(context.Background(), key, p)
			require.Error(t, err)
			require.True(t, reader_error.ForTestsClassifiedReaderLayer(err), "expected sink wrapper, got: %v", err)
		})
	}
}

type pusherWithErr struct{ err error }

func (p *pusherWithErr) IsEmpty() bool                                   { return true }
func (p *pusherWithErr) Ack(s3_pusher.Chunk) (bool, error)               { return true, nil }
func (p *pusherWithErr) Push(_ context.Context, _ s3_pusher.Chunk) error { return p.err }

type sinkItemsPusher struct {
	items []abstract.ChangeItem
}

func (p *sinkItemsPusher) IsEmpty() bool                     { return true }
func (p *sinkItemsPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *sinkItemsPusher) Push(_ context.Context, ch s3_pusher.Chunk) error {
	p.items = append(p.items, ch.Items...)
	return nil
}
