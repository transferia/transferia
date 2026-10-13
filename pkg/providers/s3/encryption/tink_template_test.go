package encryption

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	gcmhkdfpb "github.com/tink-crypto/tink-go/v2/proto/aes_gcm_hkdf_streaming_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
	"github.com/tink-crypto/tink-go/v2/streamingaead"
	"github.com/tink-crypto/tink-go/v2/tink"
	"google.golang.org/protobuf/proto"
)

// tinkStreamingAEAD builds what a consumer outside Data Transfer builds: a Tink keyset of the standard template
// AES256_GCM_HKDF_1MB with the raw key, used through the public keyset API.
func tinkStreamingAEAD(t *testing.T, material []byte) tink.StreamingAEAD {
	template := streamingaead.AES256GCMHKDF1MBKeyTemplate()
	keyFormat := new(gcmhkdfpb.AesGcmHkdfStreamingKeyFormat)
	require.NoError(t, proto.Unmarshal(template.GetValue(), keyFormat))
	require.Equal(t, uint32(KeySize), keyFormat.GetKeySize())

	serializedKey, err := proto.Marshal(&gcmhkdfpb.AesGcmHkdfStreamingKey{
		Version:  0,
		Params:   keyFormat.GetParams(),
		KeyValue: material,
	})
	require.NoError(t, err)
	handle := insecurecleartextkeyset.KeysetHandle(&tinkpb.Keyset{
		PrimaryKeyId: 1,
		Key: []*tinkpb.Keyset_Key{{
			KeyData: &tinkpb.KeyData{
				TypeUrl:         template.GetTypeUrl(),
				Value:           serializedKey,
				KeyMaterialType: tinkpb.KeyData_SYMMETRIC,
			},
			Status:           tinkpb.KeyStatusType_ENABLED,
			KeyId:            1,
			OutputPrefixType: template.GetOutputPrefixType(),
		}},
	})
	streamingAEAD, err := streamingaead.New(handle)
	require.NoError(t, err)
	return streamingAEAD
}

func TestFormatIsTinkTemplate(t *testing.T) {
	key := randomKey(t)
	tinkAEAD := tinkStreamingAEAD(t, key.material)
	// Several segments, so that segment boundaries and the last-segment flag are covered too.
	plaintext := randomBytes(t, firstSegmentPlaintextSize+segmentPlaintextSize+12345)

	t.Run("Tink decrypts our objects with the key only", func(t *testing.T) {
		reader, err := tinkAEAD.NewDecryptingReader(bytes.NewReader(encrypt(t, key, plaintext)), nil)
		require.NoError(t, err)
		decrypted, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(plaintext, decrypted))
	})

	t.Run("we decrypt objects of Tink", func(t *testing.T) {
		var ciphertext bytes.Buffer
		writer, err := tinkAEAD.NewEncryptingWriter(&ciphertext, nil)
		require.NoError(t, err)
		_, err = writer.Write(plaintext)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		decrypted, err := decryptSequentially(key, ciphertext.Bytes())
		require.NoError(t, err)
		require.True(t, bytes.Equal(plaintext, decrypted))
		decrypted, err = decryptWithReaderAt(key, ciphertext.Bytes())
		require.NoError(t, err)
		require.True(t, bytes.Equal(plaintext, decrypted))
	})
}
