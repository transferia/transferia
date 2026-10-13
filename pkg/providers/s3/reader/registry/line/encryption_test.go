package line

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	parent_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type chunkRecordingPusher struct {
	rows      int
	completed bool
}

func (p *chunkRecordingPusher) IsEmpty() bool                     { return true }
func (p *chunkRecordingPusher) Ack(s3_pusher.Chunk) (bool, error) { return true, nil }
func (p *chunkRecordingPusher) Push(_ context.Context, chunk s3_pusher.Chunk) error {
	p.rows += len(chunk.Items)
	p.completed = p.completed || chunk.Completed
	return nil
}

type nopWriteCloser struct {
	strings.Builder
}

func (*nopWriteCloser) Close() error { return nil }

func readEncryptedLines(t *testing.T, key *encryption.Key, object []byte) (*chunkRecordingPusher, error) {
	const objectKey = "l/part.txt"
	builder := s3raw.NewMemoryS3RawReaderBuilder(map[string][]byte{objectKey: object}, key)
	src := &s3_model.S3Source{
		TableNamespace:   "ns",
		TableName:        "t",
		Bucket:           "b",
		PathPrefix:       "l",
		InputFormat:      model.ParsingFormatLine,
		UnparsedPolicy:   s3_model.UnparsedPolicyContinue,
		HideSystemCols:   true,
		ConnectionConfig: s3_model.ConnectionConfig{AccessKey: "a", SecretKey: "b"},
		// A fixed schema, so that the bucket is not listed.
		OutputSchema: []abstract.ColSchema{abstract.NewColSchema("row", ytschema.TypeBytes, false)},
	}
	src.WithDefaults()
	sess := fake_s3.NewSess()
	st := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	read, err := NewLineReader(src, logger.Log, sess, st, builder)
	require.NoError(t, err)
	schema, err := NewLineSchemaResolver(src, logger.Log, sess, st, builder)
	require.NoError(t, err)

	pusher := &chunkRecordingPusher{rows: 0, completed: false}
	return pusher, parent_reader.NewReaderContractor(read, schema).Read(context.Background(), objectKey, pusher)
}

// TestLineReaderFailsOnTruncatedEncryptedObject checks that an encrypted object truncated after a non-final segment
// and one more byte fails instead of being completed with the rows of its first segment.
func TestLineReaderFailsOnTruncatedEncryptedObject(t *testing.T) {
	key, err := encryption.ParseKey(model.SecretString(strings.Repeat("0f", encryption.KeySize)))
	require.NoError(t, err)
	var ciphertext nopWriteCloser
	writer := encryption.NewEncryptingWriter(&ciphertext, key)
	_, err = writer.Write([]byte(strings.Repeat("a\n", 1000000)))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	object := []byte(ciphertext.String())

	t.Run("whole object", func(t *testing.T) {
		pusher, err := readEncryptedLines(t, key, object)
		require.NoError(t, err)
		require.Equal(t, 1000000, pusher.rows)
		require.True(t, pusher.completed)
	})
	t.Run("truncated after a segment and one byte", func(t *testing.T) {
		pusher, err := readEncryptedLines(t, key, object[:1024*1024+1])
		require.ErrorIs(t, err, encryption.ErrAuthentication)
		require.True(t, abstract.IsFatal(err), "a decryption failure must not be retried: %v", err)
		require.False(t, pusher.completed)
	})
	t.Run("another key", func(t *testing.T) {
		otherKey, err := encryption.ParseKey(model.SecretString(strings.Repeat("1e", encryption.KeySize)))
		require.NoError(t, err)
		pusher, err := readEncryptedLines(t, otherKey, object)
		require.ErrorIs(t, err, encryption.ErrAuthentication)
		require.True(t, abstract.IsFatal(err), "a decryption failure must not be retried: %v", err)
		require.Zero(t, pusher.rows)
		require.False(t, pusher.completed)
	})
}
