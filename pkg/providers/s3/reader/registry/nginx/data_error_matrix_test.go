package reader

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
)

type errNginxBuild struct{ client s3iface.S3API }

func (e errNginxBuild) BuildReader(
	_ context.Context,
	_ s3iface.S3API,
	_ string,
	_ string,
	_ *stats.SourceStats,
) (s3raw.S3RawReader, error) {
	return nil, xerrors.New("forced transport")
}
func (e errNginxBuild) BuildClient(_ *aws_session.Session) s3iface.S3API { return e.client }

// badNginxLogLine is not a valid line for the fixed log_format (missing quoted fields).
const badNginxKey = "access.log"

var badNginxLine = []byte("this is not a valid nginx line for the format\n")

func newNginxSource(t *testing.T, pol s3_model.UnparsedPolicy) *s3_model.S3Source {
	t.Helper()
	src := &s3_model.S3Source{
		TableNamespace: "ns",
		TableName:      "t",
		Bucket:         "b",
		InputFormat:    model.ParsingFormatNginx,
		UnparsedPolicy: pol,
		ReadBatchSize:  10,
		Format: s3_model.Format{NginxSetting: &s3_model.NginxSetting{
			Format:    ` "$remote_addr" $status`,
			BlockSize: 4096,
		}},
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	src.WithDefaults()
	return src
}

func newNginxReader(t *testing.T, src *s3_model.S3Source, b s3raw.S3RawReaderBuilder) s3_reader.Reader {
	t.Helper()
	sess := fake_s3.NewSess()
	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewNginxReader(src, logger.Log, sess, st, b)
	require.NoError(t, err)
	schema, err := NewNginxSchemaResolver(src, logger.Log, sess, st, b)
	require.NoError(t, err)
	return s3_reader.NewReaderContractor(read, schema)
}

// TestNginxBadLine_Policies: parse failure goes through HandleDataError (Continue/Fail/Retry).
func TestNginxBadLine_Policies(t *testing.T) {
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(badNginxKey, badNginxLine, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)

	t.Run("continue", func(t *testing.T) {
		src := newNginxSource(t, s3_model.UnparsedPolicyContinue)
		p := &sinkNginxPusher{}
		err := newNginxReader(t, src, b).Read(context.Background(), badNginxKey, p)
		require.NoError(t, err)
		var unp int
		for _, it := range p.items {
			if parsers.IsUnparsed(it) {
				unp++
			}
		}
		require.Greater(t, unp, 0)
	})
	t.Run("fail", func(t *testing.T) {
		src := newNginxSource(t, s3_model.UnparsedPolicyFail)
		err := newNginxReader(t, src, b).Read(context.Background(), badNginxKey, &sinkNginxPusher{})
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
	})
	t.Run("retry", func(t *testing.T) {
		src := newNginxSource(t, s3_model.UnparsedPolicyRetry)
		err := newNginxReader(t, src, b).Read(context.Background(), badNginxKey, &sinkNginxPusher{})
		require.Error(t, err)
		require.False(t, abstract.IsFatal(err))
		_, ok := reader_error.AsReaderErrorData(err)
		require.True(t, ok, "expected data error, got: %v", err)
	})
}

// TestNginxRead_TransportErrorIgnoresUnparsedPolicy: transport is never gated by data policy.
func TestNginxRead_TransportErrorIgnoresUnparsedPolicy(t *testing.T) {
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyContinue, s3_model.UnparsedPolicyFail, s3_model.UnparsedPolicyRetry,
	} {
		t.Run((string)(pol), func(t *testing.T) {
			cli := fake_s3.NewFakeS3Client(t)
			builder := errNginxBuild{client: cli}
			r := newNginxReader(t, newNginxSource(t, pol), builder)
			err := r.Read(context.Background(), "any", &sinkNginxPusher{})
			require.Error(t, err)
			require.True(t, reader_error.ForTestsClassifiedReaderLayer(err), "got: %v", err)
		})
	}
}

// TestNginxRead_SinkErrorIgnoresUnparsedPolicy: sink is never gated by data policy.
func TestNginxRead_SinkErrorIgnoresUnparsedPolicy(t *testing.T) {
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyContinue, s3_model.UnparsedPolicyFail, s3_model.UnparsedPolicyRetry,
	} {
		t.Run((string)(pol), func(t *testing.T) {
			cli := fake_s3.NewFakeS3Client(t)
			// well-formed line: two fields match format ` "$remote_addr" $status`
			body := []byte(` "127.0.0.1" 200` + "\n")
			key := "ok.log"
			cli.AddFile(fake_s3.NewFile(key, body, 1))
			b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
			r := newNginxReader(t, newNginxSource(t, pol), b)
			sinkErr := xerrors.New("push failed")
			p := &nginxPusherWithErr{err: sinkErr}
			err := r.Read(context.Background(), key, p)
			require.Error(t, err)
			require.True(t, reader_error.ForTestsClassifiedReaderLayer(err), "expected sink, got: %v", err)
		})
	}
}

type sinkNginxPusher struct{ items []abstract.ChangeItem }

func (p *sinkNginxPusher) IsEmpty() bool                     { return true }
func (p *sinkNginxPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *sinkNginxPusher) Push(_ context.Context, ch s3_pusher.Chunk) error {
	p.items = append(p.items, ch.Items...)
	return nil
}

type nginxPusherWithErr struct{ err error }

func (p *nginxPusherWithErr) IsEmpty() bool                               { return true }
func (p *nginxPusherWithErr) Ack(s3_pusher.Chunk) (bool, error)           { return true, nil }
func (p *nginxPusherWithErr) Push(context.Context, s3_pusher.Chunk) error { return p.err }
