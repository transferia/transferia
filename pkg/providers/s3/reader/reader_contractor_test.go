package reader

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type stubS3Reader struct {
	readFn func(context.Context, *abstract.TableSchema, string, s3_pusher.Pusher) reader_error.ReaderError
}

func (s *stubS3Reader) EstimateRowsCountAllObjects(_ context.Context) (uint64, error) {
	return 0, nil
}

func (s *stubS3Reader) EstimateRowsCountOneObject(_ context.Context, _ *aws_s3.Object) (uint64, error) {
	return 0, nil
}

func (s *stubS3Reader) Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	if s.readFn != nil {
		return s.readFn(ctx, schema, filePath, pusher)
	}
	return nil
}

type stubSchemaResolver struct {
	listing *SchemaWalkListing
	inferFn func(context.Context, string) (*abstract.TableSchema, reader_error.ReaderError)
}

func (s *stubSchemaResolver) SchemaWalkListing() *SchemaWalkListing { return s.listing }

func (s *stubSchemaResolver) TryInferSchemaFromObject(ctx context.Context, objectKey string) (*abstract.TableSchema, reader_error.ReaderError) {
	if s.inferFn != nil {
		return s.inferFn(ctx, objectKey)
	}
	return abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("x", ytschema.TypeString, false)}), nil
}

type recordingPusher struct {
	pushErr error
	pushed  int
}

func (p *recordingPusher) IsEmpty() bool                     { return true }
func (p *recordingPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *recordingPusher) Push(_ context.Context, _ s3_pusher.Chunk) error {
	p.pushed++
	return p.pushErr
}

// flakySink fails the first Push then succeeds (exercises completion-chunk retry).
type flakySink struct {
	pushCalls int
	firstErr  error
}

func (p *flakySink) IsEmpty() bool                     { return true }
func (p *flakySink) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *flakySink) Push(_ context.Context, _ s3_pusher.Chunk) error {
	p.pushCalls++
	if p.pushCalls == 1 {
		return p.firstErr
	}
	return nil
}

func listingWithPreset(preset *abstract.TableSchema, pol s3_model.UnparsedPolicy, cli *fake_s3.FakeS3Client) *SchemaWalkListing {
	return &SchemaWalkListing{
		TableSchema:      preset,
		Policy:           pol,
		Bucket:           "bucket",
		PathPrefix:       "pfx/",
		PathPattern:      "",
		Client:           cli,
		Logger:           logger.Log,
		ObjectsFilter:    AcceptAllObjects,
		ListOpLabel:      "stub.list",
		WrapInferErrorOp: "stub.wrap",
	}
}

func TestReaderContractor_ResolveSchemaRetriesTransportThenOK(t *testing.T) {
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile("pfx/sample.csv", []byte("h\nv\n"), 1))

	var attempt atomic.Uint32
	resolver := &stubSchemaResolver{
		listing: listingWithPreset(abstract.NewTableSchema(nil), s3_model.UnparsedPolicyContinue, cli),
		inferFn: func(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError) {
			n := attempt.Add(1)
			if n <= 2 {
				return nil, reader_error.NewReaderErrorTransport("infer", key, xerrors.New("temporary"))
			}
			return abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("h", ytschema.TypeString, false)}), nil
		},
	}

	c := NewReaderContractor(
		&stubS3Reader{},
		resolver,
		withContractorBackoff(backoff.NewConstantBackOff(0)),
	)
	ctx := context.Background()
	sch, err := c.ResolveSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, sch)
	require.GreaterOrEqual(t, attempt.Load(), uint32(3))
}

func TestReaderContractor_ReadRetriesTransportOnce(t *testing.T) {
	preset := abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("c", ytschema.TypeString, false)})
	cli := fake_s3.NewFakeS3Client(t)

	var readCalls atomic.Int32
	reg := &stubS3Reader{
		readFn: func(context.Context, *abstract.TableSchema, string, s3_pusher.Pusher) reader_error.ReaderError {
			if readCalls.Add(1) == 1 {
				return reader_error.NewReaderErrorTransport("read", "obj", xerrors.New("down"))
			}
			return nil
		},
	}
	resolver := &stubSchemaResolver{listing: listingWithPreset(preset, s3_model.UnparsedPolicyContinue, cli)}
	c := NewReaderContractor(reg, resolver, withContractorBackoff(backoff.NewConstantBackOff(0)))

	err := c.Read(context.Background(), "obj", &recordingPusher{})
	require.NoError(t, err)
	require.Equal(t, int32(2), readCalls.Load())
}

func TestReaderContractor_ReadRetriesSinkOnCompletionPush(t *testing.T) {
	preset := abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("c", ytschema.TypeString, false)})
	cli := fake_s3.NewFakeS3Client(t)
	reg := &stubS3Reader{}
	resolver := &stubSchemaResolver{listing: listingWithPreset(preset, s3_model.UnparsedPolicyContinue, cli)}
	c := NewReaderContractor(reg, resolver, withContractorBackoff(backoff.NewConstantBackOff(0)))

	sink := &flakySink{firstErr: xerrors.New("sink full")}
	err := c.Read(context.Background(), "obj", sink)
	require.NoError(t, err)
	require.Equal(t, 2, sink.pushCalls, "first completion push fails, second succeeds")
}

func TestReaderContractor_Read_NoBackoff_ReturnsTransportWithoutRetry(t *testing.T) {
	preset := abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("c", ytschema.TypeString, false)})
	cli := fake_s3.NewFakeS3Client(t)

	var readCalls atomic.Int32
	reg := &stubS3Reader{
		readFn: func(context.Context, *abstract.TableSchema, string, s3_pusher.Pusher) reader_error.ReaderError {
			readCalls.Add(1)
			return reader_error.NewReaderErrorTransport("read", "obj", xerrors.New("down"))
		},
	}
	resolver := &stubSchemaResolver{listing: listingWithPreset(preset, s3_model.UnparsedPolicyContinue, cli)}
	c := NewReaderContractor(reg, resolver, withContractorBackoff(nil))

	err := c.Read(context.Background(), "obj", &recordingPusher{})
	require.Error(t, err)
	require.Equal(t, int32(1), readCalls.Load())
}

func TestReaderContractor_Read_TransportAndSink_Classification(t *testing.T) {
	preset := abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("c", ytschema.TypeString, false)})
	cli := fake_s3.NewFakeS3Client(t)

	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyRetry,
	} {
		t.Run("transport_"+fmt.Sprint(pol), func(t *testing.T) {
			reg := &stubS3Reader{
				readFn: func(context.Context, *abstract.TableSchema, string, s3_pusher.Pusher) reader_error.ReaderError {
					return reader_error.NewReaderErrorTransport("read", "obj", xerrors.New("down"))
				},
			}
			resolver := &stubSchemaResolver{listing: listingWithPreset(preset, pol, cli)}
			c := NewReaderContractor(reg, resolver, withContractorBackoff(nil))
			err := c.Read(context.Background(), "obj", &recordingPusher{})
			require.Error(t, err)
			require.Equal(t, reader_error.ContractorKindTransport, reader_error.ClassifyContractorError(err))
		})

		t.Run("sink_"+fmt.Sprint(pol), func(t *testing.T) {
			reg := &stubS3Reader{}
			resolver := &stubSchemaResolver{listing: listingWithPreset(preset, pol, cli)}
			c := NewReaderContractor(reg, resolver, withContractorBackoff(nil))
			p := &recordingPusher{pushErr: xerrors.New("sink full")}
			err := c.Read(context.Background(), "obj", p)
			require.Error(t, err)
			require.Equal(t, reader_error.ContractorKindSink, reader_error.ClassifyContractorError(err))
		})
	}
}
