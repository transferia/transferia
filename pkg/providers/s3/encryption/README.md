# Client-side encryption of S3 objects

S3 endpoints can encrypt every object they write and decrypt every object they read with a symmetric key set in the
endpoint settings, like a password (`EncryptionKey`). The bucket stores only ciphertexts, so nothing is required from
the storage: no KMS, no server-side encryption, any S3-compatible service works.

```
destination: rows -> serializer (parquet/csv/json) -> [gzip|zlib] -> encryption -> upload
source:      download -> decryption -> [gunzip|zlib] -> format reader -> rows
```

## Where it is wired

| Endpoint | Code |
|---|---|
| Destination `object_storage_v1`, snapshot and queue-to-S3 | `v1/sink/writer.NewWriter` |
| Source `object_storage` (the only S3 source), snapshot and replication | `reader/s3raw.NewS3RawReader`, key from `storage.New` and `s3sess` |

The legacy `object_storage` destination does not support encryption: it is going to be removed.

## Format

The standard Tink streaming AEAD key template `AES256_GCM_HKDF_1MB` (AES-256-GCM-HKDF, HKDF-SHA256, 1 MiB segments)
with empty associated data, see `format.go` and
https://developers.google.com/tink/streaming-aead/aes_gcm_hkdf_streaming. Any Tink implementation decrypts objects
given only the key: build a keyset of this template with the key and pass empty associated data.

- Every object gets a random salt, and its own AES-256-GCM key is derived from the user key with HKDF-SHA256.
  There is no limit on the amount of data per user key.
- Every 1 MiB segment has its own authentication tag. The segment index and the last-segment flag are part of the
  nonce: a corrupted, reordered, truncated or extended object fails decryption instead of producing data. This
  protects the bytes that the writer produced: a writer closed before all data was written makes a valid shorter
  object, and the completeness of such an object is up to the destination, as for plaintext objects.
- Overhead: 40 bytes of header per object and 16 bytes per segment (0.0015%).
- Parquet needs random access: `ReaderAt` decrypts only the segments that a read covers and reads their ciphertext
  with one range request. The last segment is authenticated when an object is opened, so a truncated object fails
  before its footer is parsed.

## Decisions

1. Whole objects are encrypted, not values. Column names and the schema are encrypted too, parquet keeps column types,
   and encryption is far cheaper than compression (see `BENCHMARK.md`). A column-level transformer would work with
   any destination, but every value would grow, and decryption would need the original column types.
2. A standard format: Tink is already vendored, objects are written by the Tink writer, and the cryptography is AES-GCM
   and HKDF from Tink and the Go standard library. The readers (`segment_cipher.go`, `stream.go`, `reader_at.go`)
   are ours: tink-go has no random access, and its sequential reader accepts a ciphertext truncated right after a
   non-final segment and one more byte. Both readers return data only from authenticated segments and succeed only
   after the final segment is authenticated.
3. Compression happens before encryption: ciphertext does not compress.
4. Object names do not change, `.gz` stays: cleanup regexps and Iceberg rely on names. Objects get the informational
   metadata `data-transfer-encryption: tink;AES256_GCM_HKDF_1MB`, it never contains key material.
5. A source with a key expects every object to be encrypted and succeeds only if every object is decrypted: a
   plaintext, damaged or truncated object, or another key fails the transfer. Such failures are `ErrAuthentication`,
   and the unparsed policy never skips them. A source without a key reads objects as they are stored.
6. The key is 32 random bytes in hex, 64 characters (`openssl rand -hex 32`), like keys of pkg/cipher. Raw bytes
   cannot be passed through text fields. Passwords are not accepted: there is no key stretching.
7. The associated data is empty, as in the Tink template: decryption needs only the key. It does not include the
   object name, so encrypted objects can be copied and renamed. As a consequence, someone who can write to the bucket
   can replace an object with another object encrypted with the same key, and an object that another tool encrypted
   with the same key and template is accepted too. Use the key for Data Transfer only. Encryption does not protect
   against substitution or deletion: use bucket access control.

## Benchmarks

See [BENCHMARK.md](BENCHMARK.md).
