package s3raw

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	mathrand "math/rand"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
	"github.com/transferia/transferia/pkg/stats"
)

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}

func newTestKey(t *testing.T) *encryption.Key {
	material := make([]byte, encryption.KeySize)
	_, err := rand.Read(material)
	require.NoError(t, err)
	key, err := encryption.ParseKey(model.SecretString(hex.EncodeToString(material)))
	require.NoError(t, err)
	return key
}

// encryptObject prepares an object as an S3 destination writes it: optionally gzipped, then encrypted.
func encryptObject(t *testing.T, key *encryption.Key, plaintext []byte, compress bool) []byte {
	var object bytes.Buffer
	encrypted := encryption.NewEncryptingWriter(nopWriteCloser{Writer: &object}, key)
	writer := encrypted
	if compress {
		writer = gzip.NewWriter(encrypted)
	}
	_, err := writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, encrypted.Close())
	return object.Bytes()
}

func newTestReader(t *testing.T, client s3iface.S3API, objectKey string, key *encryption.Key) S3RawReader {
	metrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	reader, err := NewS3RawReader(context.Background(), client, "bucket", objectKey, key, metrics)
	require.NoError(t, err)
	return reader
}

func csvRows(rows int) []byte {
	var data bytes.Buffer
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&data, "%d,user_%d@example.com,+7900%07d\n", i, i, i)
	}
	return data.Bytes()
}

func TestDecryptingReader(t *testing.T) {
	key := newTestKey(t)
	// Several encryption segments, so reads cross segment boundaries.
	plaintext := csvRows(50000)

	for _, tc := range []struct {
		name      string
		objectKey string
		compress  bool
	}{
		{name: "plain object", objectKey: "part-00000.csv", compress: false},
		{name: "gzipped object", objectKey: "part-00000.csv.gz", compress: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &MemoryS3Client{Objects: map[string][]byte{tc.objectKey: encryptObject(t, key, plaintext, tc.compress)}}
			reader := newTestReader(t, client, tc.objectKey, key)

			t.Run("sequential read", func(t *testing.T) {
				decrypted, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.True(t, bytes.Equal(plaintext, decrypted))
			})
			t.Run("read after close starts over", func(t *testing.T) {
				// Estimators read an object again after closing it.
				require.NoError(t, reader.Close())
				decrypted, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.True(t, bytes.Equal(plaintext, decrypted))
				require.NoError(t, reader.Close())
			})
			t.Run("random access", func(t *testing.T) {
				random := mathrand.New(mathrand.NewSource(1))
				for i := 0; i < 100; i++ {
					offset := random.Int63n(int64(len(plaintext)))
					buffer := make([]byte, random.Intn(600*1024))
					n, err := reader.ReadAt(buffer, offset)
					if err != nil {
						require.ErrorIs(t, err, io.EOF)
					}
					require.True(t, bytes.Equal(plaintext[offset:offset+int64(n)], buffer[:n]))
				}
			})
		})
	}
}

func TestDecryptingReaderSizeIsPlaintextSize(t *testing.T) {
	key := newTestKey(t)
	plaintext := csvRows(50000)
	client := &MemoryS3Client{Objects: map[string][]byte{"part.parquet": encryptObject(t, key, plaintext, false)}}
	reader := newTestReader(t, client, "part.parquet", key)
	require.IsType(t, &decryptingReader{}, reader)
	// parquet readers locate the footer by Size().
	require.Equal(t, int64(len(plaintext)), reader.Size())
}

func TestDecryptingReaderRejectsPlaintextObjects(t *testing.T) {
	plaintext := csvRows(1000)
	var gzipped bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipped)
	_, err := gzipWriter.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())

	client := &MemoryS3Client{Objects: map[string][]byte{"plain.csv": plaintext, "plain.csv.gz": gzipped.Bytes()}}
	key := newTestKey(t)
	for objectKey := range client.Objects {
		t.Run(objectKey, func(t *testing.T) {
			reader := newTestReader(t, client, objectKey, key)
			_, err := io.ReadAll(reader)
			require.Error(t, err)
			_, err = reader.ReadAt(make([]byte, 10), 0)
			require.Error(t, err)
		})
	}
}

func TestDecryptingReaderRejectsAnotherKey(t *testing.T) {
	client := &MemoryS3Client{Objects: map[string][]byte{"part.csv": encryptObject(t, newTestKey(t), csvRows(1000), false)}}
	reader := newTestReader(t, client, "part.csv", newTestKey(t))
	_, err := io.ReadAll(reader)
	require.Error(t, err)
}

func gzipped(t *testing.T, data []byte) []byte {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

// TestKeyAndObjectMatrix pins the contract for every combination of the source key and the stored object:
// with a key, reading succeeds only if the whole object is decrypted; without a key, the stored bytes are read as is.
func TestKeyAndObjectMatrix(t *testing.T) {
	key := newTestKey(t)
	plaintext := csvRows(50000)
	encrypted := encryptObject(t, key, plaintext, false)
	encryptedGzipped := encryptObject(t, key, plaintext, true)

	for _, tc := range []struct {
		name      string
		objectKey string
		object    []byte
		key       *encryption.Key
		expected  []byte // nil means that reading must fail
	}{
		{name: "key, encrypted", objectKey: "a.csv", object: encrypted, key: key, expected: plaintext},
		{name: "key, encrypted gzip", objectKey: "a.csv.gz", object: encryptedGzipped, key: key, expected: plaintext},
		{name: "key, plaintext", objectKey: "a.csv", object: plaintext, key: key, expected: nil},
		{name: "key, plaintext gzip", objectKey: "a.csv.gz", object: gzipped(t, plaintext), key: key, expected: nil},
		{name: "another key, encrypted", objectKey: "a.csv", object: encrypted, key: newTestKey(t), expected: nil},
		{name: "key, encrypted truncated after a segment and one byte", objectKey: "a.csv", object: encrypted[:1024*1024+1], key: key, expected: nil},
		{name: "no key, plaintext", objectKey: "a.csv", object: plaintext, key: nil, expected: plaintext},
		{name: "no key, plaintext gzip", objectKey: "a.csv.gz", object: gzipped(t, plaintext), key: nil, expected: plaintext},
		{name: "no key, encrypted", objectKey: "a.csv", object: encrypted, key: nil, expected: encrypted},
		{name: "no key, encrypted gzip", objectKey: "a.csv.gz", object: encryptedGzipped, key: nil, expected: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &MemoryS3Client{Objects: map[string][]byte{tc.objectKey: tc.object}}
			read, err := io.ReadAll(newTestReader(t, client, tc.objectKey, tc.key))
			if tc.expected == nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, bytes.Equal(tc.expected, read))
		})
	}
}
