package parquet

import (
	"bytes"
	"context"
	mathrand "math/rand"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
)

// failingDecryptionReader behaves like a reader of an encrypted object with a wrong key or a damaged segment.
type failingDecryptionReader struct {
	s3raw.S3RawReader
}

func (r failingDecryptionReader) ReadAt([]byte, int64) (int, error) {
	return 0, encryption.ErrAuthentication.Wrap(xerrors.New("segment 2"))
}

type failingDecryptionBuilder struct {
	s3raw.S3RawReaderBuilder
}

func (b failingDecryptionBuilder) BuildReader(ctx context.Context, client s3iface.S3API, bucket string, key string, metrics *stats.SourceStats) (s3raw.S3RawReader, error) {
	reader, err := b.S3RawReaderBuilder.BuildReader(ctx, client, bucket, key, metrics)
	if err != nil {
		return nil, err
	}
	return failingDecryptionReader{S3RawReader: reader}, nil
}

// TestParquetDecryptionErrorIsFatalWithContinue checks that a decryption failure is not skipped as a malformed file:
// with UnparsedPolicyContinue a skipped object would be acknowledged without its rows.
func TestParquetDecryptionErrorIsFatalWithContinue(t *testing.T) {
	cli := fake_s3.NewFakeS3Client(t)
	cli.AddFile(fake_s3.NewFile(corruptParquetObject, corruptParquetBytes, 1))
	builder := failingDecryptionBuilder{S3RawReaderBuilder: s3raw.NewFakeS3RawReaderBuilder(cli, cli)}
	reader := newParqReader(t, s3_model.UnparsedPolicyContinue, builder)

	pusher := &accPusher{}
	err := reader.Read(context.Background(), corruptParquetObject, pusher)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err), "expected a fatal error: %v", err)
	require.ErrorIs(t, err, encryption.ErrAuthentication)
	require.Empty(t, pusher.items, "no unparsed markers or rows are expected")
}

type parquetRow struct {
	C int64 `parquet:"c"`
}

type nopWriteCloser struct {
	bytes.Buffer
}

func (*nopWriteCloser) Close() error { return nil }

// encryptedParquet writes a real parquet file of several encryption segments and encrypts it as an S3 destination does.
func encryptedParquet(t *testing.T, key *encryption.Key, rowCount int) []byte {
	random := mathrand.New(mathrand.NewSource(1))
	rows := make([]parquetRow, rowCount)
	for i := range rows {
		rows[i] = parquetRow{C: random.Int63()}
	}
	var plaintext bytes.Buffer
	require.NoError(t, parquet.Write(&plaintext, rows))

	var ciphertext nopWriteCloser
	writer := encryption.NewEncryptingWriter(&ciphertext, key)
	_, err := writer.Write(plaintext.Bytes())
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return ciphertext.Bytes()
}

func parseTestKey(t *testing.T, hexByte string) *encryption.Key {
	key, err := encryption.ParseKey(model.SecretString(strings.Repeat(hexByte, encryption.KeySize)))
	require.NoError(t, err)
	return key
}

// TestEncryptedParquetWithContinue reads a real encrypted parquet file: the right key gives all rows, while another key
// or a damaged segment is fatal even with UnparsedPolicyContinue.
func TestEncryptedParquetWithContinue(t *testing.T) {
	const objectKey = "part.parquet"
	const rowCount = 600000
	key := parseTestKey(t, "0f")
	object := encryptedParquet(t, key, rowCount)
	require.Greater(t, len(object), 3*1024*1024, "the file must span several segments")

	damaged := bytes.Clone(object)
	damaged[len(damaged)/2] ^= 0x01

	for _, tc := range []struct {
		name   string
		object []byte
		key    *encryption.Key
	}{
		{name: "right key", object: object, key: key},
		{name: "another key", object: object, key: parseTestKey(t, "a1")},
		{name: "damaged middle segment", object: damaged, key: key},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := s3raw.NewMemoryS3RawReaderBuilder(map[string][]byte{objectKey: tc.object}, tc.key)
			reader := newParqReader(t, s3_model.UnparsedPolicyContinue, builder)
			pusher := &accPusher{}
			err := reader.Read(context.Background(), objectKey, pusher)
			if tc.name == "right key" {
				require.NoError(t, err)
				require.Len(t, pusher.items, rowCount)
				return
			}
			require.True(t, abstract.IsFatal(err), "expected a fatal error: %v", err)
			require.ErrorIs(t, err, encryption.ErrAuthentication)
			for _, item := range pusher.items {
				require.False(t, parsers.IsUnparsed(item), "the object must not be skipped as unparsed")
			}
		})
	}
}
