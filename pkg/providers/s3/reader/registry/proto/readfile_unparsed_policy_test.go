package proto

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	cloud_exportpb "github.com/transferia/transferia/metrika/proto/cloud_export"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	parent_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// fileDescriptorSetBytesForMessage builds a FileDescriptorSet (with imports) for dynamic proto registration.
func fileDescriptorSetBytesForMessage(m protoreflect.Message) ([]byte, error) {
	fd := m.Descriptor().ParentFile()
	seen := make(map[protoreflect.FileDescriptor]struct{})
	var files []*descriptorpb.FileDescriptorProto
	var walk func(protoreflect.FileDescriptor) error
	walk = func(f protoreflect.FileDescriptor) error {
		if f == nil {
			return nil
		}
		if _, ok := seen[f]; ok {
			return nil
		}
		imps := f.Imports()
		for i := 0; i < imps.Len(); i++ {
			if err := walk(imps.Get(i)); err != nil {
				return err
			}
		}
		seen[f] = struct{}{}
		files = append(files, protodesc.ToFileDescriptorProto(f))
		return nil
	}
	if err := walk(fd); err != nil {
		return nil, err
	}
	return proto.Marshal(&descriptorpb.FileDescriptorSet{File: files})
}

const oneMetrikaItemSize = 2183

func metrikaOutputSchema(t *testing.T) []abstract.ColSchema {
	t.Helper()
	cfg := MetrikaHitProtoseqConfig()
	builder, err := protoparser.NewLazyProtoParserBuilder(cfg, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, err)
	p := builder.BuildBaseParser()
	items := p.Do(constructMessage(time.Now(), metrikaData[:oneMetrikaItemSize], nil), abstract.NewEmptyPartition())
	require.NotEmpty(t, items)
	cols := items[0].TableSchema.Columns()
	out := make([]abstract.ColSchema, len(cols))
	copy(out, cols)
	return out
}

func newMetrikaProtoSource(t *testing.T, pol s3_model.UnparsedPolicy) *s3_model.S3Source {
	t.Helper()
	msg := new(cloud_exportpb.CloudTransferHit)
	desc, err := fileDescriptorSetBytesForMessage(msg.ProtoReflect())
	require.NoError(t, err)
	cfg := MetrikaHitProtoseqConfig()
	src := &s3_model.S3Source{
		TableNamespace: "ns",
		TableName:      "t",
		Bucket:         "b",
		InputFormat:    model.ParsingFormatPROTO,
		UnparsedPolicy: pol,
		OutputSchema:   metrikaOutputSchema(t),
		Format: s3_model.Format{ProtoParser: &s3_model.ProtoSetting{
			DescFile:       desc,
			MessageName:    "CloudTransferHit",
			PackageType:    protoparser.PackageTypeProtoseq,
			IncludeColumns: cfg.IncludeColumns,
			PrimaryKeys:    cfg.PrimaryKeys,
		}},
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
	}
	src.WithDefaults()
	return src
}

// TestProtoReadFileTruncatedRecord_UnparsedPolicies: small file (non-stream) with incomplete protobuf
// yields parser unparsed — readFileAndParse must route policies through parser.Next path.
func TestProtoReadFileTruncatedRecord_UnparsedPolicies(t *testing.T) {
	key := "bad.proto.bin"
	// Truncated record cannot decode into a full row.
	body := metrikaData[:200]
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(key, body, 1))
	b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
	sess := fake_s3.NewSess()

	t.Run("continue", func(t *testing.T) {
		src := newMetrikaProtoSource(t, s3_model.UnparsedPolicyContinue)
		st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
		read, err := NewProtoReader(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		schema, err := NewProtoSchemaResolver(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		r := parent_reader.NewReaderContractor(read, schema)
		p := &itemsPusherProto{}
		err = r.Read(context.Background(), key, p)
		require.NoError(t, err)
		var n int
		for _, it := range p.items {
			if parsers.IsUnparsed(it) {
				n++
			}
		}
		require.Greater(t, n, 0, "expected unparsed item(s) for truncated proto with Continue")
	})
	t.Run("fail", func(t *testing.T) {
		src := newMetrikaProtoSource(t, s3_model.UnparsedPolicyFail)
		st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
		read, err := NewProtoReader(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		schema, err := NewProtoSchemaResolver(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		r := parent_reader.NewReaderContractor(read, schema)
		err = r.Read(context.Background(), key, &itemsPusherProto{})
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
	})
	t.Run("retry", func(t *testing.T) {
		src := newMetrikaProtoSource(t, s3_model.UnparsedPolicyRetry)
		st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
		read, err := NewProtoReader(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		schema, err := NewProtoSchemaResolver(src, logger.Log, sess, st, b)
		require.NoError(t, err)
		r := parent_reader.NewReaderContractor(read, schema)
		err = r.Read(context.Background(), key, &itemsPusherProto{})
		require.Error(t, err)
		require.False(t, abstract.IsFatal(err))
		_, ok := s3_reader.AsReaderErrorData(err)
		require.True(t, ok, "expected data error, got: %v", err)
	})
}

type itemsPusherProto struct{ items []abstract.ChangeItem }

func (p *itemsPusherProto) IsEmpty() bool                     { return true }
func (p *itemsPusherProto) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *itemsPusherProto) Push(_ context.Context, ch s3_pusher.Chunk) error {
	p.items = append(p.items, ch.Items...)
	return nil
}
