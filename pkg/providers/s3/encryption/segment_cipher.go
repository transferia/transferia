package encryption

import (
	"crypto/aes"
	"crypto/cipher"

	tinksubtle "github.com/tink-crypto/tink-go/v2/subtle"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// ErrAuthentication means that an object cannot be decrypted: it is corrupted, truncated, has trailing data or is
// encrypted with another key. Readers must fail on it instead of skipping the object as malformed data.
var ErrAuthentication = xerrors.NewSentinel("authentication failed: the object is corrupted, truncated or encrypted with another key")

// segmentCipher decrypts the segments of one encrypted object. Both readers use it, so the format is implemented once.
type segmentCipher struct {
	aead        cipher.AEAD
	noncePrefix []byte
}

// newSegmentCipher parses the header of an object and derives the object key from the user key and the salt.
func newSegmentCipher(header []byte, key *Key) (*segmentCipher, error) {
	if len(header) != headerSize || header[0] != headerSize {
		return nil, ErrAuthentication.Wrap(xerrors.New("invalid encryption header, the object may be not encrypted"))
	}
	salt := header[1 : 1+saltSize]
	objectKey, err := tinksubtle.ComputeHKDF(hkdfHash, key.material, salt, associatedData, derivedKeySize)
	if err != nil {
		return nil, xerrors.Errorf("unable to derive object key: %w", err)
	}
	block, err := aes.NewCipher(objectKey)
	if err != nil {
		return nil, xerrors.Errorf("unable to create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCMWithTagSize(block, tagSize)
	if err != nil {
		return nil, xerrors.Errorf("unable to create AES-GCM: %w", err)
	}
	return &segmentCipher{
		aead:        aead,
		noncePrefix: append([]byte(nil), header[1+saltSize:]...),
	}, nil
}

// open decrypts a segment into dst, which may be ciphertext[:0] to decrypt in place. The last flag must be set for the
// final segment of the object only: it makes truncation at a segment boundary fail authentication.
func (c *segmentCipher) open(dst []byte, ciphertext []byte, index int64, last bool) ([]byte, error) {
	if len(ciphertext) < tagSize {
		return nil, xerrors.Errorf("segment %d is shorter than its authentication tag: %w", index+1, ErrAuthentication)
	}
	plaintext, err := c.aead.Open(dst, segmentNonce(c.noncePrefix, index, last), ciphertext, nil)
	if err != nil {
		return nil, xerrors.Errorf("segment %d: %w", index+1, ErrAuthentication)
	}
	return plaintext, nil
}
