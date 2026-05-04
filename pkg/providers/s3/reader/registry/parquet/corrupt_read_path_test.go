package parquet

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
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

// corruptParquetObject is not a valid Parquet file; read-path should classify as DataFile + UnparsedPolicy.
const corruptParquetObject = "bad.pq"

var corruptParquetBytes = []byte{0x50, 0x41, 0x52, 0x31, 0xde, 0xad, 0xbe, 0xef}

type accPusher struct{ items []abstract.ChangeItem }

func (p *accPusher) IsEmpty() bool                     { return true }
func (p *accPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *accPusher) Push(_ context.Context, ch s3_pusher.Chunk) error {
	p.items = append(p.items, ch.Items...)
	return nil
}

func newParqReader(t *testing.T, pol s3_model.UnparsedPolicy, b s3raw.S3RawReaderBuilder) s3_reader.Reader {
	t.Helper()
	src := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "",
		InputFormat:      model.ParsingFormatPARQUET,
		UnparsedPolicy:   pol,
		ReadBatchSize:    8,
		Format:           s3_model.Format{ParquetSetting: &s3_model.ParquetSetting{}},
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
		// Fixed schema so ResolveSchema does not open S3 before Read (corrupt-bytes cases are exercised in Read).
		OutputSchema: []abstract.ColSchema{abstract.NewColSchema("c", ytschema.TypeInt64, false)},
	}
	src.WithDefaults()
	sess := fake_s3.NewSess()
	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewParquetReader(src, logger.Log, sess, st, b)
	require.NoError(t, err)
	schema, err := NewParquetSchemaResolver(src, logger.Log, sess, st, b)
	require.NoError(t, err)
	return s3_reader.NewReaderContractor(read, schema)
}

func TestParquetCorruptObject_ReadPathContinueUnparsed(t *testing.T) {
	key := corruptParquetObject
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, corruptParquetBytes, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	r := newParqReader(t, s3_model.UnparsedPolicyContinue, b)
	p := &accPusher{}
	err := r.Read(context.Background(), key, p)
	require.NoError(t, err)
	var unp int
	for _, it := range p.items {
		if parsers.IsUnparsed(it) {
			unp++
		}
	}
	require.Greater(t, unp, 0, "expected file-level unparsed for corrupt Parquet with Continue")
}

func TestParquetCorruptObject_ReadPathFail(t *testing.T) {
	key := corruptParquetObject
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, corruptParquetBytes, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	r := newParqReader(t, s3_model.UnparsedPolicyFail, b)
	err := r.Read(context.Background(), key, &accPusher{})
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err), "expected fatal error for corrupt Parquet with Fail: %v", err)
}

func TestParquetCorruptObject_ReadPathRetry(t *testing.T) {
	key := corruptParquetObject
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, corruptParquetBytes, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	r := newParqReader(t, s3_model.UnparsedPolicyRetry, b)
	err := r.Read(context.Background(), key, &accPusher{})
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err), "expected non-fatal for corrupt Parquet with Retry: %v", err)
	_, ok := reader_error.AsReaderErrorData(err)
	require.True(t, ok, "expected data error: %v", err)
}
