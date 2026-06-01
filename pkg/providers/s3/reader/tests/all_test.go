package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	yslices "github.com/transferia/transferia/library/go/slices"
	cloud_exportpb "github.com/transferia/transferia/metrika/proto/cloud_export"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	s3_reader_registry "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ---
type MockPusher struct {
	ChangeItems []abstract.ChangeItem
}

func (p *MockPusher) IsEmpty() bool {
	return true
}

func (p *MockPusher) Push(_ context.Context, chunk s3_pusher.Chunk) error {
	p.ChangeItems = append(p.ChangeItems, chunk.Items...)
	return nil
}

func (p *MockPusher) Ack(_ s3_pusher.Chunk) (bool, error) {
	return true, nil
}

func NewMockPusher() *MockPusher {
	return &MockPusher{
		ChangeItems: make([]abstract.ChangeItem, 0),
	}
}

func TestReaderRegistryAllFormats(t *testing.T) {
	formats := s3_reader_registry.RegisteredFormats()
	require.GreaterOrEqual(t, len(formats), 7, "add new format tests in registry/ when adding a format")
	seen := make(map[string]int)
	for _, f := range formats {
		seen[f]++
	}
	for f, n := range seen {
		require.Equal(t, 1, n, "format %q registered more than once", f)
	}
}

// --- all-registered bad-entity contract (base.md) ---
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
		// $B build tree: fall back to source next to this test in checkout.
		candidates = append(candidates, filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "registry", "proto", "gotest", "metrika-data", "metrika_hit_protoseq_data.bin")))
	}
	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err == nil {
			return b
		}
	}
	require.Fail(t, "metrika fixture not found; tried %v (set ARCADIA_SOURCE_ROOT or run from full arcadia tree)", candidates)
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

func metrikaParserConfigContract() *protoparser.ProtoParserConfig {
	requiredHitsV2Columns := []string{
		"CounterID", "EventDate", "CounterUserIDHash", "UTCEventTime", "WatchID", "Sign", "HitVersion",
	}
	optionalHitsV2Columns := []string{
		"AdvEngineID", "AdvEngineStrID", "BrowserCountry", "BrowserEngineID", "BrowserEngineStrID",
		"BrowserEngineVersion1", "BrowserEngineVersion2", "BrowserEngineVersion3", "BrowserEngineVersion4",
		"BrowserLanguage", "CLID", "ClientIP", "ClientIP6", "ClientTimeZone", "CookieEnable", "DevicePixelRatio",
		"DirectCLID", "Ecommerce", "FirstPartyCookie", "FromTag", "GCLID", "GoalsReached", "HasGCLID", "HTTPError",
		"IsArtifical", "IsDownload", "IsIFrame", "IsLink", "IsMobile", "IsNotBounce", "IsPageView", "IsParameter",
		"IsTablet", "IsTV", "JavascriptEnable", "MessengerID", "MessengerStrID", "MobilePhoneModel",
		"MobilePhoneVendor", "MobilePhoneVendorStr", "NetworkType", "NetworkTypeStr", "OpenstatAdID",
		"OpenstatCampaignID", "OpenstatServiceName", "OpenstatSourceID", "OriginalURL", "OS", "OSFamily", "OSName",
		"OSRoot", "OSRootStr", "OSStr", "PageCharset", "PageViewID", "Params", "ParsedParams.Key1",
		"ParsedParams.Key10", "ParsedParams.Key2", "ParsedParams.Key3", "ParsedParams.Key4", "ParsedParams.Key5",
		"ParsedParams.Key6", "ParsedParams.Key7", "ParsedParams.Key8", "ParsedParams.Key9", "ParsedParams.Quantity",
		"QRCodeProviderID", "QRCodeProviderStrID", "RecommendationSystemID", "RecommendationSystemStrID", "Referer",
		"RegionID", "ResolutionDepth", "ResolutionHeight", "ResolutionWidth", "SearchEngineID", "SearchEngineRootID",
		"SearchEngineRootStrID", "SearchEngineStrID", "ShareService", "ShareTitle", "ShareURL", "SocialSourceNetworkID",
		"SocialSourceNetworkStrID", "SocialSourcePage", "ThirdPartyCookieEnable", "Title", "TrafficSourceID",
		"TrafficSourceStrID", "URL", "UserAgent", "UserAgentMajor", "UserAgentStr", "UserAgentVersion2",
		"UserAgentVersion3", "UserAgentVersion4", "UTMCampaign", "UTMContent", "UTMMedium", "UTMSource", "UTMTerm",
		"WindowClientHeight", "WindowClientWidth", "YQRID",
	}
	metrikaNameToProto := func(name string) string { return strings.ReplaceAll(name, ".", "_") }
	primaryKeys := yslices.Map(requiredHitsV2Columns, metrikaNameToProto)
	optionalColumns := yslices.Map(optionalHitsV2Columns, metrikaNameToProto)
	allColumns := append(
		yslices.Map(primaryKeys, protoparser.RequiredColumn),
		yslices.Map(optionalColumns, protoparser.OptionalColumn)...,
	)
	msg := new(cloud_exportpb.CloudTransferHit)
	return &protoparser.ProtoParserConfig{
		IncludeColumns:     allColumns,
		PrimaryKeys:        primaryKeys,
		ScannerMessageDesc: msg.ProtoReflect().Descriptor(),
		ProtoMessageDesc:   msg.ProtoReflect().Descriptor(),
		ProtoScannerType:   protoscanner.ScannerTypeLineSplitter,
		LineSplitter:       abstract.LfLineSplitterProtoseq,
	}
}

func mustMetrikaOutCols(t *testing.T, oneItem []byte) []abstract.ColSchema {
	t.Helper()
	cfg := metrikaParserConfigContract()
	p, err := protoparser.NewProtoParser(cfg, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, err)
	items := p.Do(
		parsers.Message{
			Offset:     0,
			SeqNo:      0,
			Key:        nil,
			CreateTime: time.Now(),
			WriteTime:  time.Now(),
			Value:      oneItem,
		}, abstract.NewEmptyPartition(),
	)
	require.NotEmpty(t, items)
	cols := items[0].TableSchema.Columns()
	out := make([]abstract.ColSchema, len(cols))
	copy(out, cols)
	return out
}

// badEntityFixture matches TestReaderAllRegisteredBadEntityFixturesContinue per-format setup.
type badEntityFixture struct {
	key             string
	body            []byte
	schemaBuddyKey  string
	schemaBuddyBody []byte
	fill            func(s *s3_model.S3Source)
	need            int
}

func newBadEntityFixtures(t *testing.T) map[string]badEntityFixture {
	t.Helper()
	metrika := metrikaDataFromArcadia(t)
	const oneMetrikaItem = 2183
	cfgProto := metrikaParserConfigContract()
	msg := new(cloud_exportpb.CloudTransferHit)
	desc, err := fileDescriptorSetBytes(msg.ProtoReflect())
	require.NoError(t, err)
	return map[string]badEntityFixture{
		string(model.ParsingFormatCSV): {
			key:             "c.csv",
			body:            []byte("need,x\n\"unclosed-quoted-field\n"),
			schemaBuddyKey:  "c_schema_buddy.csv",
			schemaBuddyBody: []byte("need,x\n3,4\n"),
			fill: func(s *s3_model.S3Source) {
				s.Format.CSVSetting = &s3_model.CSVSetting{
					Delimiter: ",", BlockSize: 1024,
					AdditionalReaderOptions: s3_model.AdditionalOptions{
						IncludeColumns:        []string{"need"},
						IncludeMissingColumns: false,
					},
				}
			},
			need: 1,
		},
		string(model.ParsingFormatJSON): {
			key:  "j.json",
			body: []byte(`{"a":"nope"}` + "\n"),
			fill: func(s *s3_model.S3Source) {
				// Ignore (not Infer): with preset OutputSchema + system cols, Infer adds _rest and can yield
				// exactly four logical columns — GenericParser.restIDX() breaks at len(columns)==4.
				s.Format.JSONLSetting = &s3_model.JSONLSetting{
					BlockSize:               4096,
					NewlinesInValue:         false,
					UnexpectedFieldBehavior: s3_model.Ignore,
				}
				col := abstract.NewColSchema("a", ytschema.TypeInt64, false)
				col.Path = "a"
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
			},
			need: 1,
		},
		string(model.ParsingFormatJSONLine): {
			key:  "jl.jsonl",
			body: []byte(`{"a":"nope"}` + "\n"),
			fill: func(s *s3_model.S3Source) {
				s.Format.JSONLSetting = &s3_model.JSONLSetting{
					BlockSize:               4096,
					UnexpectedFieldBehavior: s3_model.Ignore,
				}
				col := abstract.NewColSchema("a", ytschema.TypeInt64, false)
				col.Path = "a"
				col.Required = true
				s.OutputSchema = []abstract.ColSchema{col}
			},
			need: 1,
		},
		string(model.ParsingFormatLine): {
			key:  "l/x.txt",
			body: []byte("NaN\n"),
			fill: func(s *s3_model.S3Source) {
				s.PathPrefix = "l"
				col := abstract.NewColSchema("row", ytschema.TypeUint64, false)
				s.OutputSchema = []abstract.ColSchema{col}
				s.HideSystemCols = true
			},
			need: 1,
		},
		string(model.ParsingFormatNginx): {
			key:  "a.log",
			body: []byte("not a valid line for format\n"),
			fill: func(s *s3_model.S3Source) {
				s.Format.NginxSetting = &s3_model.NginxSetting{
					Format:    ` "$remote_addr" $status`,
					BlockSize: 4096,
				}
			},
			need: 1,
		},
		string(model.ParsingFormatPARQUET): {
			key:            "b.pq",
			body:           []byte{0x50, 0x41, 0x52, 0x31, 0xde, 0xad, 0xbe, 0xef},
			schemaBuddyKey: "b_schema_buddy.pq",
			fill: func(s *s3_model.S3Source) {
				s.Format.ParquetSetting = &s3_model.ParquetSetting{}
			},
			need: 1,
		},
		string(model.ParsingFormatPROTO): {
			key:  "p.bin",
			body: metrika[:200],
			fill: func(s *s3_model.S3Source) {
				s.Format.ProtoParser = &s3_model.ProtoSetting{
					DescFile:       desc,
					MessageName:    "CloudTransferHit",
					PackageType:    protoparser.PackageTypeProtoseq,
					IncludeColumns: cfgProto.IncludeColumns,
					PrimaryKeys:    cfgProto.PrimaryKeys,
				}
				s.OutputSchema = mustMetrikaOut(t, metrika[:oneMetrikaItem])
			},
			need: 1,
		},
	}
}

// TestNoSuchFileHandleMode: for every registered format, schema resolves (buddy or preset) but the target
// object is absent at Read time. Parsers must return ReaderErrorNoSuchFile; Continue skips, Fatal stops.
func TestNoSuchFileHandleMode(t *testing.T) {
	fixtures := newBadEntityFixtures(t)
	registered := s3_reader_registry.RegisteredFormats()
	modes := []struct {
		name  string
		mode  s3_model.NoSuchFileHandleMode
		fatal bool
	}{
		{"continue", s3_model.NoSuchFileHandleModeContinue, false},
		{"fatal", s3_model.NoSuchFileHandleModeFatal, true},
	}
	sess := fake_s3.NewSess()
	statsR := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	for _, pol := range modes {
		for _, f := range registered {
			c := fixtures[f]
			t.Run(fmt.Sprintf("%s_%s", f, pol.name), func(t *testing.T) {
				cli := fake_s3.NewFakeS3Client(t)
				if c.schemaBuddyKey != "" {
					buddyBody := c.schemaBuddyBody
					if model.ParsingFormat(f) == model.ParsingFormatPARQUET {
						buddyBody = minimalValidParquetBytes(t)
					}
					cli.AddFile(fake_s3.NewFile(c.schemaBuddyKey, buddyBody, 1))
				}
				b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
				src := &s3_model.S3Source{
					TableNamespace:       "ns",
					TableName:            "t",
					Bucket:               "b",
					InputFormat:          model.ParsingFormat(f),
					NoSuchFileHandleMode: pol.mode,
					ConnectionConfig:     s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
				}
				c.fill(src)
				src.WithDefaults()
				require.Equal(t, pol.mode, src.NoSuchFileHandleMode)
				r, err := s3_reader_registry.NewReader(src, logger.Log, sess, statsR, b)
				require.NoError(t, err)
				ctx := context.Background()
				sch, rerr := r.ResolveSchema(ctx)
				require.NoError(t, rerr)
				require.NotNil(t, sch)
				require.NotEmpty(t, sch.Columns())
				p := NewMockPusher()
				readErr := r.Read(ctx, c.key, p)
				_, ok := reader_error.AsReaderErrorNoSuchFile(readErr)
				require.True(t, ok, "format %s: expected ReaderErrorNoSuchFile when object is missing", f)
				appliedErr := reader_error.ApplyNoSuchFileHandleMode(logger.Log, src.NoSuchFileHandleMode, readErr)
				if pol.fatal {
					require.Error(t, appliedErr)
					require.True(t, abstract.IsFatal(appliedErr))
				} else {
					require.NoError(t, appliedErr)
				}
			})
		}
	}
}

// TestUnparsedPolicy: for every registered format and every UnparsedPolicy, ResolveSchema then Read on a fake S3
// client using an intentionally invalid body (schema buddy + corrupt/unparsable main object): Continue must emit
// *_unparsed; Fail / Retry must return a Read error (fatal vs retryable per policy).
func TestUnparsedPolicy(t *testing.T) {
	fixtures := newBadEntityFixtures(t)
	registered := s3_reader_registry.RegisteredFormats()
	regSet := make(map[string]struct{})
	for _, f := range registered {
		regSet[f] = struct{}{}
	}
	fixSet := make(map[string]struct{})
	for f := range fixtures {
		fixSet[f] = struct{}{}
	}
	require.Equal(t, regSet, fixSet, "fixtures must cover RegisteredFormats exactly")

	sess := fake_s3.NewSess()
	statsR := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	policies := []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyRetry,
	}
	for _, pol := range policies {
		for _, f := range registered {
			c := fixtures[f]
			t.Run(fmt.Sprintf("%s_%s", f, pol), func(t *testing.T) {
				cli := fake_s3.NewFakeS3Client(t)
				if c.schemaBuddyKey != "" {
					buddyBody := c.schemaBuddyBody
					if model.ParsingFormat(f) == model.ParsingFormatPARQUET {
						buddyBody = minimalValidParquetBytes(t)
					}
					cli.AddFile(fake_s3.NewFile(c.schemaBuddyKey, buddyBody, 1))
				}
				cli.AddFile(fake_s3.NewFile(c.key, c.body, 1))
				b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
				src := &s3_model.S3Source{
					TableNamespace:   "ns",
					TableName:        "t",
					Bucket:           "b",
					InputFormat:      model.ParsingFormat(f),
					UnparsedPolicy:   pol,
					ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
				}
				c.fill(src)
				src.WithDefaults()
				r, err := s3_reader_registry.NewReader(src, logger.Log, sess, statsR, b)
				require.NoError(t, err)
				ctx := context.Background()
				sch, rerr := r.ResolveSchema(ctx)
				require.NoError(t, rerr)
				require.NotNil(t, sch)
				require.NotEmpty(t, sch.Columns())
				p := NewMockPusher()
				err = r.Read(ctx, c.key, p)
				switch pol {
				case s3_model.UnparsedPolicyContinue:
					require.NoError(t, err)
					n := 0
					for _, it := range p.ChangeItems {
						if parsers.IsUnparsed(it) {
							n++
						}
					}
					require.GreaterOrEqual(t, n, c.need, "format %s: expected *_unparsed rows for invalid body", f)
				case s3_model.UnparsedPolicyFail, s3_model.UnparsedPolicyRetry:
					require.Error(t, err)
					if pol == s3_model.UnparsedPolicyFail {
						require.True(t, abstract.IsFatal(err))
					} else {
						require.False(t, abstract.IsFatal(err))
					}
				default:
					t.Fatalf("unexpected policy %v", pol)
				}
			})
		}
	}
}

// TestReaderAllRegisteredBadEntityFixturesContinue: for each format in RegisteredFormats there is a
// logical "bad" entity fixture; with UnparsedPolicyContinue, Read must not return an error, and
// if the format emits data-layer unparsed for the fixture, pusher must see *_unparsed.
func TestReaderAllRegisteredBadEntityFixturesContinue(t *testing.T) {
	sess := fake_s3.NewSess()
	statsR := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	bad := newBadEntityFixtures(t)
	registered := s3_reader_registry.RegisteredFormats()
	regSet := make(map[string]struct{})
	for _, f := range registered {
		regSet[f] = struct{}{}
	}
	fixSet := make(map[string]struct{})
	for f := range bad {
		fixSet[f] = struct{}{}
	}
	require.Equal(t, regSet, fixSet, "update bad-entity map when adding/removing a reader format (registered vs fixtures)")
	for _, f := range registered {
		c := bad[f]
		t.Run(f, func(t *testing.T) {
			cli := fake_s3.NewFakeS3Client(t)
			if c.schemaBuddyKey != "" {
				buddyBody := c.schemaBuddyBody
				if model.ParsingFormat(f) == model.ParsingFormatPARQUET {
					buddyBody = minimalValidParquetBytes(t)
				}
				cli.AddFile(fake_s3.NewFile(c.schemaBuddyKey, buddyBody, 1))
			}
			cli.AddFile(fake_s3.NewFile(c.key, c.body, 1))
			b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
			src := &s3_model.S3Source{
				TableNamespace:   "ns",
				TableName:        "t",
				Bucket:           "b",
				InputFormat:      model.ParsingFormat(f),
				UnparsedPolicy:   s3_model.UnparsedPolicyContinue,
				ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
			}
			c.fill(src)
			src.WithDefaults()
			r, err := s3_reader_registry.NewReader(src, logger.Log, sess, statsR, b)
			require.NoError(t, err)
			p := NewMockPusher()
			err = r.Read(context.Background(), c.key, p)
			require.NoError(t, err)
			if c.need == 0 {
				return
			}
			var n int
			for _, it := range p.ChangeItems {
				if parsers.IsUnparsed(it) {
					n++
				}
			}
			require.GreaterOrEqual(t, n, c.need, "format %s should surface *_unparsed for its bad-entity fixture", f)
		})
	}
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

// mustMetrikaOut alias
func mustMetrikaOut(t *testing.T, oneItem []byte) []abstract.ColSchema {
	return mustMetrikaOutCols(t, oneItem)
}

// TestResolveSchemaWalkSkipsUnparsableFirstObjectContinue: with UnparsedPolicyContinue, schema inference
// moves to the next object when the first yields a data-layer error and cannot produce a schema.
func TestResolveSchemaWalkSkipsUnparsableFirstObjectContinue(t *testing.T) {
	metrika := metrikaDataFromArcadia(t)
	const oneMetrikaItem = 2183
	msg := new(cloud_exportpb.CloudTransferHit)
	desc, err := fileDescriptorSetBytes(msg.ProtoReflect())
	require.NoError(t, err)
	cfgProto := metrikaParserConfigContract()
	corruptParquet := []byte{0x50, 0x41, 0x52, 0x31, 0xde, 0xad, 0xbe, 0xef}
	goodParquet := minimalValidParquetBytes(t)

	sess := fake_s3.NewSess()
	statsR := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	type walkCase struct {
		name   string
		format model.ParsingFormat
		fill   func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client)
	}
	cases := []walkCase{
		{
			name:   "CSV",
			format: model.ParsingFormatCSV,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				s.Format.CSVSetting = &s3_model.CSVSetting{
					Delimiter: ",", BlockSize: 1024,
					AdditionalReaderOptions: s3_model.AdditionalOptions{
						IncludeColumns:        []string{"need"},
						IncludeMissingColumns: false,
					},
				}
				cli.AddFile(fake_s3.NewFile("pfx/0bad.csv", []byte("a,b\n1,2\n"), 1))
				cli.AddFile(fake_s3.NewFile("pfx/1good.csv", []byte("need,z\n3,4\n"), 1))
			},
		},
		{
			name:   "JSON",
			format: model.ParsingFormatJSON,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				s.Format.JSONLSetting = &s3_model.JSONLSetting{BlockSize: 4096, NewlinesInValue: false, UnexpectedFieldBehavior: s3_model.Infer}
				cli.AddFile(fake_s3.NewFile("pfx/0bad.json", []byte("{\n"), 1))
				cli.AddFile(fake_s3.NewFile("pfx/1good.json", []byte(`{"k":1}`+"\n"), 1))
			},
		},
		{
			name:   "JSONL",
			format: model.ParsingFormatJSONLine,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				s.Format.JSONLSetting = &s3_model.JSONLSetting{BlockSize: 4096, NewlinesInValue: false, UnexpectedFieldBehavior: s3_model.Infer}
				cli.AddFile(fake_s3.NewFile("pfx/0bad.jsonl", []byte("{\n"), 1))
				cli.AddFile(fake_s3.NewFile("pfx/1good.jsonl", []byte(`{"k":1}`+"\n"), 1))
			},
		},
		{
			name:   "LINE",
			format: model.ParsingFormatLine,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				cli.AddFile(fake_s3.NewFile("pfx/0a.txt", []byte("x\n"), 1))
				cli.AddFile(fake_s3.NewFile("pfx/1b.txt", []byte("y\n"), 1))
			},
		},
		{
			name:   "PARQUET",
			format: model.ParsingFormatPARQUET,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				s.Format.ParquetSetting = &s3_model.ParquetSetting{}
				cli.AddFile(fake_s3.NewFile("pfx/0bad.pq", corruptParquet, 1))
				cli.AddFile(fake_s3.NewFile("pfx/1good.pq", goodParquet, 1))
			},
		},
		{
			name:   "PROTO",
			format: model.ParsingFormatPROTO,
			fill: func(s *s3_model.S3Source, cli *fake_s3.FakeS3Client) {
				s.Format.ProtoParser = &s3_model.ProtoSetting{
					DescFile:       desc,
					MessageName:    "CloudTransferHit",
					PackageType:    protoparser.PackageTypeProtoseq,
					IncludeColumns: cfgProto.IncludeColumns,
					PrimaryKeys:    cfgProto.PrimaryKeys,
				}
				cli.AddFile(fake_s3.NewFile("pfx/0bad.bin", []byte("not-protobuf"), 1))
				cli.AddFile(fake_s3.NewFile("pfx/1good.bin", metrika[:oneMetrikaItem], 1))
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cli := fake_s3.NewFakeS3Client(t)
			b := s3raw.NewFakeS3RawReaderBuilder(cli, cli)
			src := &s3_model.S3Source{
				TableNamespace:   "ns",
				TableName:        "t",
				Bucket:           "b",
				PathPrefix:       "pfx/",
				InputFormat:      c.format,
				UnparsedPolicy:   s3_model.UnparsedPolicyContinue,
				ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
			}
			c.fill(src, cli)
			src.WithDefaults()
			r, err := s3_reader_registry.NewReader(src, logger.Log, sess, statsR, b)
			require.NoError(t, err)
			sch, rerr := r.ResolveSchema(context.Background())
			require.NoError(t, rerr)
			require.NotNil(t, sch)
			require.NotEmpty(t, sch.Columns(), "format %s should resolve schema (second object when first is bad)", c.format)
		})
	}
}
