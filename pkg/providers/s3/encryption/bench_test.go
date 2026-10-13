package encryption

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/streamingaead/subtle"
	"golang.org/x/crypto/chacha20poly1305"
)

// benchmarkPayloadSize is large enough to include many segments of every compared size.
const benchmarkPayloadSize = 32 * 1024 * 1024

var benchmarkSegmentSizes = []int{64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024}

// csvLikePayload resembles serialized rows, so compression ratios and speeds are realistic.
func csvLikePayload(size int) []byte {
	var payload strings.Builder
	for row := 0; payload.Len() < size; row++ {
		fmt.Fprintf(&payload, "%d,user_%d@example.com,+7900%07d,2025-10-%02d 12:%02d:00,%d.%02d\n", row, row, row%10000000, row%28+1, row%60, row*7, row%100)
	}
	return []byte(payload.String()[:size])
}

type streamingAEAD interface {
	NewEncryptingWriter(w io.Writer, aad []byte) (io.WriteCloser, error)
	NewDecryptingReader(r io.Reader, aad []byte) (io.Reader, error)
}

func streamingAEADs(b *testing.B, segmentSize int) map[string]streamingAEAD {
	key := randomKey(b).material
	gcm, err := subtle.NewAESGCMHKDF(key, "SHA256", 32, segmentSize, 0)
	require.NoError(b, err)
	ctr, err := subtle.NewAESCTRHMAC(key, "SHA256", 32, "SHA256", 32, segmentSize, 0)
	require.NoError(b, err)
	return map[string]streamingAEAD{
		"aes256-gcm-hkdf":       gcm,
		"aes256-ctr-hmacsha256": ctr,
	}
}

// BenchmarkStreamingAEAD compares Tink streaming schemes and segment sizes on whole-object encryption and decryption.
func BenchmarkStreamingAEAD(b *testing.B) {
	payload := csvLikePayload(benchmarkPayloadSize)
	for _, segmentSize := range benchmarkSegmentSizes {
		for name, scheme := range streamingAEADs(b, segmentSize) {
			b.Run(fmt.Sprintf("encrypt/%s/segment=%dKiB", name, segmentSize/1024), func(b *testing.B) {
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					writer, err := scheme.NewEncryptingWriter(io.Discard, associatedData)
					require.NoError(b, err)
					_, err = writer.Write(payload)
					require.NoError(b, err)
					require.NoError(b, writer.Close())
				}
			})

			var ciphertext bytes.Buffer
			writer, err := scheme.NewEncryptingWriter(&ciphertext, associatedData)
			require.NoError(b, err)
			_, err = writer.Write(payload)
			require.NoError(b, err)
			require.NoError(b, writer.Close())

			b.Run(fmt.Sprintf("decrypt/%s/segment=%dKiB", name, segmentSize/1024), func(b *testing.B) {
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					reader, err := scheme.NewDecryptingReader(bytes.NewReader(ciphertext.Bytes()), associatedData)
					require.NoError(b, err)
					_, err = io.Copy(io.Discard, reader)
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkAEADPrimitives shows the raw speed of AEAD primitives on one segment, without the streaming format.
func BenchmarkAEADPrimitives(b *testing.B) {
	key := randomKey(b).material
	block, err := aes.NewCipher(key)
	require.NoError(b, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(b, err)
	chacha, err := chacha20poly1305.New(key)
	require.NoError(b, err)
	xchacha, err := chacha20poly1305.NewX(key)
	require.NoError(b, err)
	primitives := map[string]cipher.AEAD{
		"aes256-gcm":        gcm,
		"chacha20poly1305":  chacha,
		"xchacha20poly1305": xchacha,
	}

	for _, size := range []int{4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024} {
		plaintext := csvLikePayload(size)
		for name, aead := range primitives {
			nonce := make([]byte, aead.NonceSize())
			dst := make([]byte, 0, size+aead.Overhead())
			b.Run(fmt.Sprintf("seal/%s/%dKiB", name, size/1024), func(b *testing.B) {
				b.SetBytes(int64(size))
				for i := 0; i < b.N; i++ {
					dst = aead.Seal(dst[:0], nonce, plaintext, nil)
				}
			})
		}
	}
}

// BenchmarkObjectWriter measures what an S3 sink pays per object: compression and encryption of serialized rows.
func BenchmarkObjectWriter(b *testing.B) {
	payload := csvLikePayload(benchmarkPayloadSize)
	key := randomKey(b)
	cases := []struct {
		name    string
		newSink func(dst io.Writer) (io.Writer, func() error)
	}{
		{
			name: "plain",
			newSink: func(dst io.Writer) (io.Writer, func() error) {
				return dst, func() error { return nil }
			},
		},
		{
			name: "encrypted",
			newSink: func(dst io.Writer) (io.Writer, func() error) {
				encrypted := NewEncryptingWriter(nopCloser{Writer: dst}, key)
				return encrypted, encrypted.Close
			},
		},
		{
			name: "gzip",
			newSink: func(dst io.Writer) (io.Writer, func() error) {
				compressed := gzip.NewWriter(dst)
				return compressed, compressed.Close
			},
		},
		{
			name: "gzip+encrypted",
			newSink: func(dst io.Writer) (io.Writer, func() error) {
				encrypted := NewEncryptingWriter(nopCloser{Writer: dst}, key)
				compressed := gzip.NewWriter(encrypted)
				return compressed, func() error {
					if err := compressed.Close(); err != nil {
						return err
					}
					return encrypted.Close()
				}
			},
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				writer, closeWriter := tc.newSink(io.Discard)
				_, err := writer.Write(payload)
				require.NoError(b, err)
				require.NoError(b, closeWriter())
			}
		})
	}
}

// BenchmarkReaderAt measures parquet-like access: opening an object (header and last segment) and reading the footer,
// then random reads of column chunk pages.
func BenchmarkReaderAt(b *testing.B) {
	key := randomKey(b)
	ciphertext := encrypt(b, key, csvLikePayload(benchmarkPayloadSize))
	source := bytes.NewReader(ciphertext)

	b.Run("open and read footer", func(b *testing.B) {
		footer := make([]byte, 64*1024)
		for i := 0; i < b.N; i++ {
			reader, err := NewReaderAt(source, int64(len(ciphertext)), key)
			require.NoError(b, err)
			_, err = reader.ReadAt(footer, reader.Size()-int64(len(footer)))
			require.NoError(b, err)
		}
	})

	reader, err := NewReaderAt(source, int64(len(ciphertext)), key)
	require.NoError(b, err)
	for _, pageSize := range []int{8 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("sequential pages of %dKiB", pageSize/1024), func(b *testing.B) {
			page := make([]byte, pageSize)
			b.SetBytes(int64(pageSize))
			offset := int64(0)
			for i := 0; i < b.N; i++ {
				if offset+int64(pageSize) > reader.Size() {
					offset = 0
				}
				_, err := reader.ReadAt(page, offset)
				require.NoError(b, err)
				offset += int64(pageSize)
			}
		})
	}
}
