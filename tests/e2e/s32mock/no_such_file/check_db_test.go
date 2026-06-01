package nosuchfile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	cloud_exportpb "github.com/transferia/transferia/metrika/proto/cloud_export"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	s3_reader_registry "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	s3util_list "github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	s3_storage "github.com/transferia/transferia/pkg/providers/s3/storage"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type noSuchFixture struct {
	format      model.ParsingFormat
	ext         string
	keepBody    []byte
	missingBody []byte
	fill        func(*s3_model.S3Source)
}

type noSuchMode struct {
	name  string
	mode  s3_model.NoSuchFileHandleMode
	fatal bool
}

func TestNoSuchFileHandleModeE2E(t *testing.T) {
	t.Setenv("YC", "1")

	fixtures := newNoSuchFileFixtures(t)
	registered := s3_reader_registry.RegisteredFormats()
	regSet := make(map[string]struct{}, len(registered))
	for _, f := range registered {
		regSet[f] = struct{}{}
	}
	fixSet := make(map[string]struct{}, len(fixtures))
	for f := range fixtures {
		fixSet[f] = struct{}{}
	}
	require.Equal(t, regSet, fixSet, "fixtures must cover RegisteredFormats exactly")

	modes := []noSuchMode{
		{name: "continue", mode: s3_model.NoSuchFileHandleModeContinue, fatal: false},
		{name: "fatal", mode: s3_model.NoSuchFileHandleModeFatal, fatal: true},
	}

	for _, pol := range modes {
		for _, f := range registered {
			c := fixtures[f]
			t.Run(fmt.Sprintf("%s_%s", f, pol.name), func(t *testing.T) {
				t.Run("storage", func(t *testing.T) {
					runStorageCase(t, c, pol)
				})
				t.Run("source", func(t *testing.T) {
					runSourceCase(t, c, pol)
				})
			})
		}
	}
}

func runStorageCase(t *testing.T, c noSuchFixture, mode noSuchMode) {
	prefix := fmt.Sprintf("nosuch/storage/%s/%s", strings.ToLower(string(c.format)), mode.name)
	src, keepKey, missingKey := prepareSource(t, c, mode.mode, prefix)
	uploadOrderedFiles(t, src, keepKey, c.keepBody, missingKey, c.missingBody)

	registry := helpers.EmptyRegistry()
	files, err := s3util_list.ListAll(context.Background(), logger.Log, registry, src)
	require.NoError(t, err)

	var listed []string
	for _, f := range files {
		listed = append(listed, f.FileName)
	}
	require.Contains(t, listed, keepKey)
	require.Contains(t, listed, missingKey)

	filterJSON, err := json.Marshal([]string{keepKey, missingKey})
	require.NoError(t, err)

	storage, err := s3_storage.New(src, transferID(t), logger.Log, registry)
	require.NoError(t, err)
	defer storage.Close()

	var deleted atomic.Bool
	pusher := func([]abstract.ChangeItem) error {
		if deleted.CompareAndSwap(false, true) {
			deleteObject(t, src, missingKey)
		}
		return nil
	}

	err = storage.LoadTable(context.Background(), abstract.TableDescription{
		Schema: src.TableNamespace,
		Name:   src.TableName,
		Filter: abstract.WhereStatement(string(filterJSON)),
	}, pusher)
	assertNoSuchMode(t, mode, err)
}

func runSourceCase(t *testing.T, c noSuchFixture, mode noSuchMode) {
	prefix := fmt.Sprintf("nosuch/source/%s/%s", strings.ToLower(string(c.format)), mode.name)
	src, keepKey, missingKey := prepareSource(t, c, mode.mode, prefix)
	uploadOrderedFiles(t, src, keepKey, c.keepBody, missingKey, c.missingBody)

	cp := coordinator.NewStatefulFakeClient()
	runtime := abstract.NewFakeShardingTaskRuntime(0, 1, 1, 1)
	fetcher, ctx, cancel, reader, _, err := object_fetcher.NewWrapped(context.Background(), logger.Log, helpers.EmptyRegistry(), src, transferID(t), cp, runtime)
	require.NoError(t, err)
	defer cancel()

	files, err := fetcher.FetchObjects(reader)
	require.NoError(t, err)

	deleteObject(t, src, missingKey)

	var readErr reader_error.ReaderError
	pusher := s3_pusher.New(func([]abstract.ChangeItem) error { return nil }, nil, logger.Log, 0)
	for _, obj := range files {
		readErr = reader.Read(ctx, obj.FileName, pusher)
		readErr = reader_error.ApplyNoSuchFileHandleMode(logger.Log, src.NoSuchFileHandleMode, readErr)
		if readErr != nil {
			break
		}
	}

	assertNoSuchMode(t, mode, readErr)
}

func assertNoSuchMode(t *testing.T, mode noSuchMode, err error) {
	if mode.fatal {
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
		return
	}
	require.NoError(t, err)
}

func prepareSource(t *testing.T, c noSuchFixture, mode s3_model.NoSuchFileHandleMode, prefix string) (*s3_model.S3Source, string, string) {
	bucket := fmt.Sprintf("nosuch-%s", strings.ToLower(string(c.format)))
	src := s3recipe.PrepareCfg(t, bucket, c.format)
	src.PathPrefix = prefix
	src.TableNamespace = "ns"
	src.TableName = "t"
	src.UnparsedPolicy = s3_model.UnparsedPolicyFail
	src.NoSuchFileHandleMode = mode
	c.fill(src)
	src.WithDefaults()

	keepKey := path.Join(prefix, "00_keep"+c.ext)
	missingKey := path.Join(prefix, "01_missing"+c.ext)
	return src, keepKey, missingKey
}

func uploadOrderedFiles(t *testing.T, src *s3_model.S3Source, keepKey string, keepBody []byte, missingKey string, missingBody []byte) {
	s3recipe.UploadOneFromMemory(t, src, keepKey, keepBody)
	time.Sleep(time.Second)
	s3recipe.UploadOneFromMemory(t, src, missingKey, missingBody)
}

func deleteObject(t *testing.T, src *s3_model.S3Source, key string) {
	t.Helper()
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey,
			string(src.ConnectionConfig.SecretKey),
			"",
		),
	})
	require.NoError(t, err)
	_, err = aws_s3.New(sess).DeleteObject(&aws_s3.DeleteObjectInput{
		Bucket: aws.String(src.Bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

func transferID(t *testing.T) string {
	return helpers.GenerateTransferID(strings.ReplaceAll(t.Name(), "/", "_"))
}

func newNoSuchFileFixtures(t *testing.T) map[string]noSuchFixture {
	t.Helper()
	metrika := metrikaDataFromArcadia(t)
	const oneMetrikaItem = 2183
	msg := new(cloud_exportpb.CloudTransferHit)
	desc, err := fileDescriptorSetBytes(msg.ProtoReflect())
	require.NoError(t, err)
	nginxLine := `"127.0.0.1" "200"` + "\n"
	return map[string]noSuchFixture{
		string(model.ParsingFormatCSV): {
			format:      model.ParsingFormatCSV,
			ext:         ".csv",
			keepBody:    []byte("a\n1"),
			missingBody: []byte("a\n2"),
			fill: func(s *s3_model.S3Source) {
				s.Format.CSVSetting = &s3_model.CSVSetting{Delimiter: ",", BlockSize: 1024}
				col := abstract.NewColSchema("a", ytschema.TypeString, true)
				s.OutputSchema = []abstract.ColSchema{col}
			},
		},
		string(model.ParsingFormatJSON): {
			format:      model.ParsingFormatJSON,
			ext:         ".json",
			keepBody:    []byte(`{"a":1}` + "\n"),
			missingBody: []byte(`{"a":2}` + "\n"),
			fill: func(s *s3_model.S3Source) {
				s.Format.JSONLSetting = &s3_model.JSONLSetting{
					BlockSize:               4096,
					NewlinesInValue:         false,
					UnexpectedFieldBehavior: s3_model.Ignore,
				}
				col := abstract.NewColSchema("a", ytschema.TypeInt64, true)
				col.Path = "a"
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
			},
		},
		string(model.ParsingFormatJSONLine): {
			format:      model.ParsingFormatJSONLine,
			ext:         ".jsonl",
			keepBody:    []byte(`{"a":1}` + "\n"),
			missingBody: []byte(`{"a":2}` + "\n"),
			fill: func(s *s3_model.S3Source) {
				s.Format.JSONLSetting = &s3_model.JSONLSetting{
					BlockSize:               4096,
					NewlinesInValue:         false,
					UnexpectedFieldBehavior: s3_model.Ignore,
				}
				col := abstract.NewColSchema("a", ytschema.TypeInt64, true)
				col.Path = "a"
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
			},
		},
		string(model.ParsingFormatLine): {
			format:      model.ParsingFormatLine,
			ext:         ".txt",
			keepBody:    []byte("hello\n"),
			missingBody: []byte("world\n"),
			fill: func(s *s3_model.S3Source) {
				col := abstract.NewColSchema("row", ytschema.TypeString, true)
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
				s.HideSystemCols = true
			},
		},
		string(model.ParsingFormatNginx): {
			format:      model.ParsingFormatNginx,
			ext:         ".log",
			keepBody:    []byte(nginxLine),
			missingBody: []byte(nginxLine),
			fill: func(s *s3_model.S3Source) {
				s.Format.NginxSetting = &s3_model.NginxSetting{
					Format:    `"$remote_addr" "$status"`,
					BlockSize: 4096,
				}
				s.OutputSchema = []abstract.ColSchema{
					{ColumnName: "remote_addr", DataType: ytschema.TypeString.String(), PrimaryKey: true},
					{ColumnName: "status", DataType: ytschema.TypeInt64.String()},
				}
			},
		},
		string(model.ParsingFormatPARQUET): {
			format:      model.ParsingFormatPARQUET,
			ext:         ".pq",
			keepBody:    minimalValidParquetBytes(t),
			missingBody: minimalValidParquetBytes(t),
			fill: func(s *s3_model.S3Source) {
				s.Format.ParquetSetting = &s3_model.ParquetSetting{}
				col := abstract.NewColSchema("x", ytschema.TypeInt64, true)
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
			},
		},
		string(model.ParsingFormatPROTO): {
			format:      model.ParsingFormatPROTO,
			ext:         ".bin",
			keepBody:    metrika[:oneMetrikaItem],
			missingBody: metrika[:oneMetrikaItem],
			fill: func(s *s3_model.S3Source) {
				s.Format.ProtoParser = &s3_model.ProtoSetting{
					DescFile:    desc,
					MessageName: "CloudTransferHit",
					PackageType: protoparser.PackageTypeProtoseq,
				}
			},
		},
	}
}

func metrikaDataFromArcadia(t *testing.T) []byte {
	t.Helper()
	relUnderTM := "transfer_manager/go/pkg/providers/s3/reader/registry/proto/gotest/metrika-data/metrika_hit_protoseq_data.bin"
	var candidates []string
	for _, env := range []string{
		"ARCADIA_SOURCE_ROOT",
		"ARCADIA_ROOT",
		"ARCADIA_ROOT_DISTBUILD",
		"ORIGINAL_SOURCE_ROOT",
	} {
		if base := os.Getenv(env); base != "" {
			candidates = append(candidates, filepath.Join(base, relUnderTM))
		}
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if ok {
		candidates = append(candidates, filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "pkg", "providers", "s3", "reader", "registry", "proto", "gotest", "metrika-data", "metrika_hit_protoseq_data.bin")))
	}
	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err == nil {
			return b
		}
	}
	require.Fail(t, "metrika fixture not found; tried %v", candidates)
	return nil
}

func fileDescriptorSetBytes(m protoreflect.Message) ([]byte, error) {
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

type parquetOneInt64Row struct {
	X int64 `parquet:"x"`
}

func minimalValidParquetBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[parquetOneInt64Row](&buf)
	_, err := w.Write([]parquetOneInt64Row{{X: 1}})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}
