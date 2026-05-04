package json

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	generic_parser "github.com/transferia/transferia/pkg/parsers/generic"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

// onlyUnparsedParser always emits an unparsed change item (base.md: JSON parser retry must return a non-fatal data error).
type onlyUnparsedParser struct{}

func (onlyUnparsedParser) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	return []abstract.ChangeItem{generic_parser.NewUnparsed(
		abstract.NewEmptyPartition(),
		"tbl",
		msg.Value,
		"forced unparsed",
		0,
		msg.Offset,
		time.Unix(0, 0),
	)}
}

func (onlyUnparsedParser) DoBatch(parsers.MessageBatch) []abstract.ChangeItem { return nil }

// TestJSONParserUnparsedRetryReturnsDataError checks base.md: unparsed + Retry => non-fatal *ReaderError (data).
func TestJSONParserUnparsedRetryReturnsDataError(t *testing.T) {
	line := []byte(`{}`)
	mockS3 := &mockS3API{data: map[string][]byte{"f.json": line}}
	testSchema := abstract.NewTableSchema(
		[]abstract.ColSchema{
			{ColumnName: "x", DataType: schema.TypeAny.String(), Path: "x", PrimaryKey: false},
		},
	)
	reader := JSONParserReader{
		table: abstract.TableID{Namespace: "ns", Name: "t"}, bucket: "b",
		client: mockS3,
		logger: logger.Log,

		hideSystemCols: true,

		batchSize: 10, pathPrefix: "",

		blockSize: 1024, pathPattern: "f.json",

		metrics:        stats.NewSourceStats(metrics.NewRegistry()),
		unparsedPolicy: s3_model.UnparsedPolicyRetry,
		parser:         onlyUnparsedParser{},
	}
	chunkP := &mockChunkPusher{pushFunc: func(context.Context, pusher.Chunk) error { return nil }}
	err := reader.Read(context.Background(), testSchema, "f.json", chunkP)
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
	_, ok := s3_reader.AsReaderErrorData(err)
	require.True(t, ok, "expected data-layer error for retry, got: %v", err)
}

func runJSONParserUnparsedPolicy(t *testing.T, pol s3_model.UnparsedPolicy) error {
	t.Helper()
	line := []byte(`{}`)
	mockS3 := &mockS3API{data: map[string][]byte{"f.json": line}}
	testSchema := abstract.NewTableSchema(
		[]abstract.ColSchema{
			{ColumnName: "x", DataType: schema.TypeAny.String(), Path: "x", PrimaryKey: false},
		},
	)
	reader := JSONParserReader{
		table: abstract.TableID{Namespace: "ns", Name: "t"}, bucket: "b",
		client: mockS3,
		logger: logger.Log,

		hideSystemCols: true,

		batchSize: 10, pathPrefix: "",

		blockSize: 1024, pathPattern: "f.json",

		metrics:        stats.NewSourceStats(metrics.NewRegistry()),
		unparsedPolicy: pol,
		parser:         onlyUnparsedParser{},
	}
	chunkP := &mockChunkPusher{pushFunc: func(context.Context, pusher.Chunk) error { return nil }}
	return reader.Read(context.Background(), testSchema, "f.json", chunkP)
}

// TestJSONParserUnparsedContinuePushesUnparsed: Continue keeps unparsed on the pusher (no Read error).
func TestJSONParserUnparsedContinuePushesUnparsed(t *testing.T) {
	var items int
	chunkP := &mockChunkPusher{pushFunc: func(_ context.Context, ch pusher.Chunk) error {
		for _, it := range ch.Items {
			if parsers.IsUnparsed(it) {
				items++
			}
		}
		return nil
	}}
	line := []byte(`{}`)
	mockS3 := &mockS3API{data: map[string][]byte{"f.json": line}}
	testSchema := abstract.NewTableSchema(
		[]abstract.ColSchema{
			{ColumnName: "x", DataType: schema.TypeAny.String(), Path: "x", PrimaryKey: false},
		},
	)
	reader := JSONParserReader{
		table: abstract.TableID{Namespace: "ns", Name: "t"}, bucket: "b",
		client: mockS3,
		logger: logger.Log,

		hideSystemCols: true,

		batchSize: 10, pathPrefix: "",

		blockSize: 1024, pathPattern: "f.json",

		metrics:        stats.NewSourceStats(metrics.NewRegistry()),
		unparsedPolicy: s3_model.UnparsedPolicyContinue,
		parser:         onlyUnparsedParser{},
	}
	err := reader.Read(context.Background(), testSchema, "f.json", chunkP)
	require.NoError(t, err)
	require.Greater(t, items, 0, "expected unparsed item(s) in chunk(s)")
}

// TestJSONParserUnparsedFailIsFatal: Fail with parser unparsed is fatal.
func TestJSONParserUnparsedFailIsFatal(t *testing.T) {
	err := runJSONParserUnparsedPolicy(t, s3_model.UnparsedPolicyFail)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
}
