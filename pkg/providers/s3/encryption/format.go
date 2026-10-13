package encryption

import (
	"encoding/binary"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// Objects are encrypted with the standard Tink streaming AEAD key template AES256_GCM_HKDF_1MB
// (https://developers.google.com/tink/streaming-aead/aes_gcm_hkdf_streaming) and empty associated data, so any Tink
// implementation decrypts them given only the key.
//
// Ciphertext layout:
//
//	| header | segment 0 | segment 1 | ... | segment n-1 |
//	header     = headerLength (1 B) | salt (32 B) | noncePrefix (7 B)
//	segment i  = AES-GCM(objectKey, nonce i, plaintext i), the last 16 bytes are the authentication tag
//	nonce i    = noncePrefix (7 B) | big-endian i (4 B) | 1 if i is the last segment else 0 (1 B)
//	objectKey  = HKDF-SHA256(user key, salt, associatedData), associatedData is empty
//
// Every segment but the last one takes exactly ciphertextSegmentSize bytes, segment 0 shares them with the header.
// The segment index and the last-segment flag in the nonce make reordering, truncation and appending detectable.
const (
	hkdfHash              = "SHA256"
	derivedKeySize        = 32
	saltSize              = derivedKeySize
	noncePrefixSize       = 7
	nonceSize             = 12
	tagSize               = 16
	headerSize            = 1 + saltSize + noncePrefixSize
	ciphertextSegmentSize = 1024 * 1024
)

// associatedData is empty, as Tink uses by default: decryption needs only the key. It does not include the object
// name, so encrypted objects stay readable after they are copied or renamed.
var associatedData = []byte{}

const (
	// MetadataKey is the S3 user metadata key that marks objects encrypted by Data Transfer. It is informational:
	// a source with a configured key expects every object to be encrypted regardless of metadata.
	MetadataKey = "data-transfer-encryption"
	// MetadataValue describes the format of encrypted objects. It never contains key material.
	MetadataValue = "tink;AES256_GCM_HKDF_1MB"
)

// PlaintextSize returns the size of the plaintext stored in a ciphertext of the given size.
func PlaintextSize(ciphertextSize int64) (int64, error) {
	if ciphertextSize < headerSize+tagSize {
		return 0, xerrors.Errorf("encrypted object is too short: %d bytes", ciphertextSize)
	}
	return ciphertextSize - headerSize - segmentCount(ciphertextSize)*tagSize, nil
}

func segmentCount(ciphertextSize int64) int64 {
	return (ciphertextSize + ciphertextSegmentSize - 1) / ciphertextSegmentSize
}

// segmentBounds returns the ciphertext range [start, end) of the segment.
func segmentBounds(index int64, ciphertextSize int64) (start int64, end int64) {
	start = index * ciphertextSegmentSize
	if index == 0 {
		start = headerSize
	}
	end = min((index+1)*ciphertextSegmentSize, ciphertextSize)
	return start, end
}

// locatePlaintext returns the index of the segment holding the plaintext byte at offset and the position of that
// byte inside the decrypted segment.
func locatePlaintext(offset int64) (index int64, inner int64) {
	const firstSegmentPlaintextSize = ciphertextSegmentSize - headerSize - tagSize
	const segmentPlaintextSize = ciphertextSegmentSize - tagSize
	if offset < firstSegmentPlaintextSize {
		return 0, offset
	}
	offset -= firstSegmentPlaintextSize
	return 1 + offset/segmentPlaintextSize, offset % segmentPlaintextSize
}

func segmentNonce(noncePrefix []byte, index int64, last bool) []byte {
	nonce := make([]byte, nonceSize)
	copy(nonce, noncePrefix)
	// Tink limits a stream to 2^32-1 segments (4 PiB with 1 MiB segments), so the index fits the 32-bit counter.
	binary.BigEndian.PutUint32(nonce[noncePrefixSize:], uint32(index))
	if last {
		nonce[noncePrefixSize+4] = 1
	}
	return nonce
}
