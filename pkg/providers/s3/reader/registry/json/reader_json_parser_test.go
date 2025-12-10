package reader

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

type mockS3API struct {
	s3iface.S3API
	data map[string][]byte
}

func (m *mockS3API) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return nil, nil
}

func (m *mockS3API) HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3API) GetObjectWithContext(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if data, ok := m.data[*input.Key]; ok {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(data)),
		}, nil
	}
	return nil, xerrors.New("object not found")
}

type mockChunkPusher struct {
	pusher.Pusher
	pushFunc func(ctx context.Context, chunk pusher.Chunk) error
}

func (m *mockChunkPusher) Push(ctx context.Context, chunk pusher.Chunk) error {
	if m.pushFunc != nil {
		return m.pushFunc(ctx, chunk)
	}
	return nil
}

func createParser(t *testing.T) parsers.Parser {
	cfg := new(jsonparser.ParserConfigJSONCommon)
	cfg.NullKeysAllowed = true
	cfg.Fields = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "Item", DataType: schema.TypeAny.String(), Path: "Item", PrimaryKey: false},
	}).Columns()
	cfg.AddDedupeKeys = false
	jsonParser, err := jsonparser.NewParserJSON(cfg, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)

	return jsonParser
}

func TestReaderJSONParserRead(t *testing.T) {
	jsonLine := []byte(`{"Item":{"OrderID":{"S":"1"},"OrderDate":{"S":"2023-07-01T12:00:00Z"},"CustomerName":{"S":"John Doe"},"OrderAmount":{"N":"3540"}}}`)

	mockS3API := &mockS3API{
		data: map[string][]byte{
			"test_json_parser_read.json": jsonLine,
		},
	}

	parser := createParser(t)
	reader := JSONParserReader{
		bucket:          "test_bucket",
		pathPrefix:      "",
		pathPattern:     "test_json_parser_read.json",
		newlinesInValue: false,
		blockSize:       1024,
		client:          mockS3API,
		downloader:      nil,
		logger:          logger.Log,
		tableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "Item", DataType: schema.TypeAny.String(), Path: "Item", PrimaryKey: false},
		}),
		metrics:        stats.NewSourceStats(metrics.NewRegistry()),
		parser:         parser,
		hideSystemCols: true,
	}
	chunkPusher := &mockChunkPusher{
		pushFunc: func(ctx context.Context, chunk pusher.Chunk) error {
			require.Equal(t, "test_json_parser_read.json", chunk.FilePath)
			require.False(t, chunk.Completed)
			require.Equal(t, uint64(2), chunk.Offset)
			require.Equal(t, int64(len(jsonLine)), chunk.Size)
			require.Len(t, chunk.Items, 1)

			return nil
		},
	}

	t.Run("read sinle line without end of line", func(t *testing.T) {
		err := reader.Read(context.Background(), "test_json_parser_read.json", chunkPusher)
		require.NoError(t, err)
	})

	t.Run("read sinle line without end of line, enable newlines in value", func(t *testing.T) {
		reader.newlinesInValue = true
		err := reader.Read(context.Background(), "test_json_parser_read.json", chunkPusher)
		require.NoError(t, err)
	})
}
