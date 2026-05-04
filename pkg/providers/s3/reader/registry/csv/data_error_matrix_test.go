package csv

import (
	"context"
	"testing"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// errOnBuildS3ReaderBuilder always fails BuildReader to exercise transport / UnparsedPolicy independence.
type errOnBuildS3ReaderBuilder struct {
	client s3iface.S3API
}

func (b errOnBuildS3ReaderBuilder) BuildReader(
	_ context.Context,
	_ s3iface.S3API,
	_ string,
	_ string,
	_ *stats.SourceStats,
) (s3raw.S3RawReader, error) {
	return nil, xerrors.New("forced transport error")
}

func (b errOnBuildS3ReaderBuilder) BuildClient(_ *aws_session.Session) s3iface.S3API { return b.client }

type pusherWithErr struct {
	err error
}

func (p *pusherWithErr) IsEmpty() bool                     { return true }
func (p *pusherWithErr) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *pusherWithErr) Push(_ context.Context, _ s3_pusher.Chunk) error {
	return p.err
}

func newTestCSVSource(policy s3_model.UnparsedPolicy) *s3_model.S3Source {
	src := &s3_model.S3Source{
		TableNamespace: "ns",
		TableName:      "t",
		Bucket:         "b",
		PathPrefix:     "",
		InputFormat:    model.ParsingFormatCSV,
		UnparsedPolicy: policy,
		Format: s3_model.Format{CSVSetting: &s3_model.CSVSetting{
			Delimiter: ",",
			BlockSize: 1 * 1024 * 1024,
		}},
	}
	src.WithDefaults()
	return src
}

func newReader(t *testing.T, src *s3_model.S3Source, builder s3raw.S3RawReaderBuilder) s3_reader.Reader {
	t.Helper()
	sess := fake_s3.NewSess()
	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewCSVReader(src, logger.Log, sess, st, builder)
	require.NoError(t, err)
	schema, err := NewCSVSchemaResolver(src, logger.Log, sess, st, builder)
	require.NoError(t, err)
	return s3_reader.NewReaderContractor(read, schema)
}

// TestCSVEmptySample_DataPolicies covers base.md: empty object → data/schema; Continue / Fail / Retry.
func TestCSVEmptySample_DataPolicies(t *testing.T) {
	key := "empty.csv"
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, nil, 1))
	builder := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	r := newReader(t, newTestCSVSource(s3_model.UnparsedPolicyContinue), builder)
	p := &sinkItemsCollector{}
	err := r.Read(context.Background(), key, p)
	require.NoError(t, err)
	var unp int
	for _, it := range p.items {
		if parsers.IsUnparsed(it) {
			unp++
		}
	}
	require.Greater(t, unp, 0, "expected *_unparsed item(s) for empty CSV with Continue (EOF/empty sample)")

	r = newReader(t, newTestCSVSource(s3_model.UnparsedPolicyFail), builder)
	pf := &pusherWithErr{err: nil}
	err = r.Read(context.Background(), key, pf)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	r = newReader(t, newTestCSVSource(s3_model.UnparsedPolicyRetry), builder)
	pr := &pusherWithErr{err: nil}
	err = r.Read(context.Background(), key, pr)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}

// TestCSVMalformedRow_DataPolicies: fixed output schema, second row does not convert to int64.
func TestCSVMalformedRow_DataPolicies(t *testing.T) {
	key := "rows.csv"
	body := []byte("1\nnot_int\n")
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	builder := s3raw.NewFakeS3RawReaderBuilder(cli, cli)

	base := newTestCSVSource(s3_model.UnparsedPolicyContinue)
	vcol := abstract.NewColSchema("v", ytschema.TypeInt64, false)
	vcol.Path = "0"
	base.OutputSchema = []abstract.ColSchema{vcol}
	base.WithDefaults()

	r := newReader(t, base, builder)
	colp := &sinkItemsCollector{}
	err := r.Read(context.Background(), key, colp)
	require.NoError(t, err)
	var badRowUnp int
	for _, it := range colp.items {
		if parsers.IsUnparsed(it) {
			badRowUnp++
		}
	}
	require.Greater(t, badRowUnp, 0, "expected row-level *_unparsed for bad CSV row with Continue")

	baseFail := newTestCSVSource(s3_model.UnparsedPolicyFail)
	baseFail.OutputSchema = base.OutputSchema
	baseFail.WithDefaults()
	r = newReader(t, baseFail, builder)
	pfail := &pusherWithErr{err: nil}
	err = r.Read(context.Background(), key, pfail)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))

	baseRetry := newTestCSVSource(s3_model.UnparsedPolicyRetry)
	baseRetry.OutputSchema = base.OutputSchema
	baseRetry.WithDefaults()
	r = newReader(t, baseRetry, builder)
	pretry := &pusherWithErr{err: nil}
	err = r.Read(context.Background(), key, pretry)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
}

// TestCSVRead_TransportErrorIgnoresUnparsedPolicy: S3/reader build failure is transport, not data policy.
func TestCSVRead_TransportErrorIgnoresUnparsed(t *testing.T) {
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyRetry,
	} {
		t.Run((string)(pol), func(t *testing.T) {
			cli := fake_s3.NewFakeS3Client(t)
			// Schema resolution lists the bucket first; seed a non-empty object so ResolveSchema succeeds and Read hits BuildReader.
			cli.AddFile(fake_s3.NewFile("seed.csv", []byte("x\n"), 1))
			builder := errOnBuildS3ReaderBuilder{client: cli}
			r := newReader(t, newTestCSVSource(pol), builder)
			p := &pusherWithErr{err: nil}
			err := r.Read(context.Background(), "any", p)
			require.Error(t, err)
			require.True(t, reader_error.ForTestsClassifiedReaderLayer(err), "expected classified reader error, got: %v", err)
		})
	}
}

// TestCSVRead_PusherErrorIgnoresUnparsedPolicy: sink error is not governed by UnparsedPolicy.
func TestCSVRead_PusherErrorIgnoresUnparsedPolicy(t *testing.T) {
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyRetry,
	} {
		t.Run((string)(pol), func(t *testing.T) {
			// One data row, no header line: with fixed OutputSchema, NewCSVReader keeps headerPresent=false
			// (see reader_csv.go), so a\n1\n would treat "a" as data and fail int coercion before FlushChunk.
			// HideSystemCols: with system columns, a strictify failure on any column would be a data error; for
			// UnparsedPolicyContinue that becomes *_unparsed items and the pusher still runs, so the test
			// would (misleadingly) see LayerSink. For Fail/Retry the same row surfaces LayerData, not the
			// pusher. We need a row that parses for every policy so only the sink error is under test.
			key := "valid_sink.csv"
			// String column: avoids any int/float strictify or batch edge cases; row must parse for
			// Fail/Retry (Continue can still surface a sink error via unparsed→flush for bad rows).
			body := []byte("ok\n")
			cli := fake_s3.NewFakeS3Client(t)
			cli.AddFile(fake_s3.NewFile(key, body, 1))
			builder := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
			base := newTestCSVSource(pol)
			vcol := abstract.NewColSchema("v", ytschema.TypeString, false)
			vcol.Path = "0"
			base.OutputSchema = []abstract.ColSchema{vcol}
			base.HideSystemCols = true
			base.WithDefaults()
			r := newReader(t, base, builder)
			sinkErr := xerrors.New("push failed")
			p := &pusherWithErr{err: sinkErr}
			err := r.Read(context.Background(), key, p)
			require.Error(t, err)
			require.True(t, reader_error.ForTestsClassifiedReaderLayer(err), "expected NewErrSink wrapper, got: %v", err)
		})
	}
}

// sinkItemsCollector is a minimal pusher that records all items (for Continue / unparsed assertions).
type sinkItemsCollector struct{ items []abstract.ChangeItem }

func (p *sinkItemsCollector) IsEmpty() bool                     { return true }
func (p *sinkItemsCollector) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *sinkItemsCollector) Push(_ context.Context, ch s3_pusher.Chunk) error {
	p.items = append(p.items, ch.Items...)
	return nil
}
