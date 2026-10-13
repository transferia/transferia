package encryption

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	mathrand "math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
)

const (
	firstSegmentPlaintextSize = ciphertextSegmentSize - headerSize - tagSize
	segmentPlaintextSize      = ciphertextSegmentSize - tagSize
)

// plaintextSizes cover empty input, segment boundaries (the first segment is shorter because of the header)
// and multi-segment objects.
var plaintextSizes = []int{
	0,
	1,
	firstSegmentPlaintextSize - 1,
	firstSegmentPlaintextSize,
	firstSegmentPlaintextSize + 1,
	firstSegmentPlaintextSize + segmentPlaintextSize,
	firstSegmentPlaintextSize + 3*segmentPlaintextSize + 12345,
}

func randomKey(t testing.TB) *Key {
	material := make([]byte, KeySize)
	_, err := rand.Read(material)
	require.NoError(t, err)
	key, err := ParseKey(model.SecretString(hex.EncodeToString(material)))
	require.NoError(t, err)
	return key
}

func randomBytes(t testing.TB, size int) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error {
	return nil
}

func encrypt(t testing.TB, key *Key, plaintext []byte) []byte {
	var ciphertext bytes.Buffer
	writer := NewEncryptingWriter(nopCloser{Writer: &ciphertext}, key)
	_, err := writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return ciphertext.Bytes()
}

func decryptSequentially(key *Key, ciphertext []byte) ([]byte, error) {
	reader, err := NewDecryptingReader(bytes.NewReader(ciphertext), key)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func decryptWithReaderAt(key *Key, ciphertext []byte) ([]byte, error) {
	reader, err := NewReaderAt(bytes.NewReader(ciphertext), int64(len(ciphertext)), key)
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, reader.Size())
	if _, err := reader.ReadAt(plaintext, 0); err != nil {
		return nil, err
	}
	return plaintext, nil
}

func TestParseKey(t *testing.T) {
	t.Run("empty key disables encryption", func(t *testing.T) {
		key, err := ParseKey("")
		require.NoError(t, err)
		require.Nil(t, key)
	})
	t.Run("valid key", func(t *testing.T) {
		key, err := ParseKey(model.SecretString(strings.Repeat("0f", KeySize)))
		require.NoError(t, err)
		require.NotNil(t, key)
	})
	t.Run("upper case hex", func(t *testing.T) {
		key, err := ParseKey(model.SecretString(strings.Repeat("AB", KeySize)))
		require.NoError(t, err)
		require.NotNil(t, key)
	})
	t.Run("not hex", func(t *testing.T) {
		raw := strings.Repeat("zz", KeySize)
		_, err := ParseKey(model.SecretString(raw))
		require.Error(t, err)
		require.NotContains(t, err.Error(), raw)
	})
	for name, raw := range map[string]string{
		"too short":          strings.Repeat("0f", KeySize-1),
		"too long":           strings.Repeat("0f", KeySize+1),
		"base64 of 32 bytes": "q7Jd0b5yX1mJbV2o8nT3uKp6wZr4sQe9hLc0aYf2gDk=",
		"with spaces":        " " + strings.Repeat("0f", KeySize) + " ",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseKey(model.SecretString(raw))
			require.Error(t, err)
			require.NotContains(t, err.Error(), raw)
		})
	}
	t.Run("formatting hides key material", func(t *testing.T) {
		key := randomKey(t)
		for _, formatted := range []string{fmt.Sprint(key), fmt.Sprintf("%v", *key), fmt.Sprintf("%+v", *key), fmt.Sprintf("%#v", *key)} {
			require.Equal(t, "***HIDDEN***", formatted)
		}
	})
}

func TestRoundTrip(t *testing.T) {
	key := randomKey(t)
	for _, size := range plaintextSizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			plaintext := randomBytes(t, size)
			ciphertext := encrypt(t, key, plaintext)

			plaintextSize, err := PlaintextSize(int64(len(ciphertext)))
			require.NoError(t, err)
			require.Equal(t, int64(size), plaintextSize)

			decrypted, err := decryptSequentially(key, ciphertext)
			require.NoError(t, err)
			require.True(t, bytes.Equal(plaintext, decrypted))

			decrypted, err = decryptWithReaderAt(key, ciphertext)
			require.NoError(t, err)
			require.True(t, bytes.Equal(plaintext, decrypted))
		})
	}
}

func TestEncryptionIsRandomized(t *testing.T) {
	key := randomKey(t)
	plaintext := []byte("the same plaintext")
	require.NotEqual(t, encrypt(t, key, plaintext), encrypt(t, key, plaintext))
}

type countingCloser struct {
	bytes.Buffer
	closeCount int
}

func (c *countingCloser) Close() error {
	c.closeCount++
	return nil
}

func TestCloseIsIdempotent(t *testing.T) {
	var ciphertext countingCloser
	writer := NewEncryptingWriter(&ciphertext, randomKey(t))
	require.NoError(t, writer.Close())
	size := ciphertext.Len()
	require.NoError(t, writer.Close())
	require.Equal(t, size, ciphertext.Len())
	require.Equal(t, 1, ciphertext.closeCount)
}

func TestWriterDoesNotWriteBeforeFirstUse(t *testing.T) {
	// S3 sinks create writers over an io.Pipe before the upload goroutine starts reading it.
	pipeReader, pipeWriter := io.Pipe()
	writer := NewEncryptingWriter(pipeWriter, randomKey(t))

	uploaded := make(chan []byte)
	go func() {
		data, _ := io.ReadAll(pipeReader)
		uploaded <- data
	}()
	_, err := writer.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NotEmpty(t, <-uploaded)
}

// requireDecryptionFails checks that both readers reject a damaged ciphertext instead of returning partial data.
func requireDecryptionFails(t *testing.T, key *Key, ciphertext []byte) {
	_, err := decryptSequentially(key, ciphertext)
	require.ErrorIs(t, err, ErrAuthentication, "sequential decryption of %d bytes must fail", len(ciphertext))
	_, err = decryptWithReaderAt(key, ciphertext)
	require.ErrorIs(t, err, ErrAuthentication, "random access decryption of %d bytes must fail", len(ciphertext))
}

func TestCorruptionIsDetected(t *testing.T) {
	key := randomKey(t)
	plaintext := randomBytes(t, firstSegmentPlaintextSize+2*segmentPlaintextSize+100)
	ciphertext := encrypt(t, key, plaintext)
	require.Equal(t, int64(4), segmentCount(int64(len(ciphertext))))

	positions := map[string]int{
		"header length": 0,
		"salt":          1,
		"nonce prefix":  1 + saltSize,
		"first segment": headerSize,
		"middle":        ciphertextSegmentSize + 100,
		"segment tag":   2*ciphertextSegmentSize - 1,
		"last segment":  len(ciphertext) - 1,
	}
	for name, position := range positions {
		t.Run("flipped bit in "+name, func(t *testing.T) {
			damaged := bytes.Clone(ciphertext)
			damaged[position] ^= 0x01
			requireDecryptionFails(t, key, damaged)
		})
	}

	t.Run("truncated", func(t *testing.T) {
		// Every segment boundary and one byte after it: a reader that looks one byte ahead to find the final segment
		// must not take a non-final segment followed by a single byte for a complete ciphertext.
		sizes := []int{0, 1, headerSize - 1, headerSize, headerSize + 1, headerSize + tagSize - 1, headerSize + tagSize, len(ciphertext) - 1}
		for boundary := ciphertextSegmentSize; boundary < len(ciphertext); boundary += ciphertextSegmentSize {
			sizes = append(sizes, boundary-1, boundary, boundary+1, boundary+tagSize)
		}
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
				requireDecryptionFails(t, key, ciphertext[:size])
			})
		}
	})
	t.Run("trailing data", func(t *testing.T) {
		requireDecryptionFails(t, key, append(bytes.Clone(ciphertext), 0))
	})
	t.Run("swapped segments", func(t *testing.T) {
		damaged := bytes.Clone(ciphertext)
		first := damaged[ciphertextSegmentSize : 2*ciphertextSegmentSize]
		second := bytes.Clone(damaged[2*ciphertextSegmentSize : 3*ciphertextSegmentSize])
		copy(damaged[2*ciphertextSegmentSize:], first)
		copy(damaged[ciphertextSegmentSize:], second)
		requireDecryptionFails(t, key, damaged)
	})
	t.Run("another key", func(t *testing.T) {
		requireDecryptionFails(t, randomKey(t), ciphertext)
	})
	t.Run("plaintext object", func(t *testing.T) {
		requireDecryptionFails(t, key, plaintext)
	})
}

func TestReaderAtRandomRanges(t *testing.T) {
	key := randomKey(t)
	plaintext := randomBytes(t, firstSegmentPlaintextSize+3*segmentPlaintextSize+777)
	ciphertext := encrypt(t, key, plaintext)
	reader, err := NewReaderAt(bytes.NewReader(ciphertext), int64(len(ciphertext)), key)
	require.NoError(t, err)

	random := mathrand.New(mathrand.NewSource(42))
	for i := 0; i < 1000; i++ {
		offset := random.Int63n(int64(len(plaintext)))
		length := random.Intn(2 * ciphertextSegmentSize)
		buffer := make([]byte, length)

		n, err := reader.ReadAt(buffer, offset)

		expected := plaintext[offset:min(offset+int64(length), int64(len(plaintext)))]
		require.Equal(t, len(expected), n)
		require.True(t, bytes.Equal(expected, buffer[:n]), "offset %d, length %d", offset, length)
		if n < length {
			require.ErrorIs(t, err, io.EOF)
		} else {
			require.NoError(t, err)
		}
	}

	n, err := reader.ReadAt(make([]byte, 1), int64(len(plaintext)))
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestReaderAtConcurrentReads(t *testing.T) {
	key := randomKey(t)
	plaintext := randomBytes(t, firstSegmentPlaintextSize+5*segmentPlaintextSize)
	ciphertext := encrypt(t, key, plaintext)
	reader, err := NewReaderAt(bytes.NewReader(ciphertext), int64(len(ciphertext)), key)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			random := mathrand.New(mathrand.NewSource(seed))
			for i := 0; i < 200; i++ {
				offset := random.Int63n(int64(len(plaintext)) - 1024)
				buffer := make([]byte, 1024)
				_, err := reader.ReadAt(buffer, offset)
				require.NoError(t, err)
				require.True(t, bytes.Equal(plaintext[offset:offset+1024], buffer))
			}
		}(int64(worker))
	}
	wg.Wait()
}

// cutReader returns the first n bytes of data and then fails like an HTTP body whose connection was closed early.
type cutReader struct {
	data []byte
	n    int
}

func (r *cutReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	m := copy(p, r.data[:r.n])
	r.data = r.data[m:]
	r.n -= m
	return m, nil
}

func TestCutStreamIsReadFailure(t *testing.T) {
	key := randomKey(t)
	ciphertext := encrypt(t, key, randomBytes(t, firstSegmentPlaintextSize+segmentPlaintextSize))
	// Inside the header, at a segment boundary and inside a segment: the object is not truncated, the read is cut,
	// so the error must stay a read failure to retry rather than become a fatal authentication failure.
	for _, n := range []int{0, headerSize - 1, ciphertextSegmentSize, ciphertextSegmentSize + 1, ciphertextSegmentSize + 100} {
		t.Run(fmt.Sprintf("%d bytes", n), func(t *testing.T) {
			reader, err := NewDecryptingReader(&cutReader{data: ciphertext, n: n}, key)
			if err == nil {
				_, err = io.ReadAll(reader)
			}
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.NotErrorIs(t, err, ErrAuthentication)
		})
	}
}
