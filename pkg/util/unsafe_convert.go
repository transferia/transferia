package util

import "unsafe"

// UnsafeStringToBytes returns a []byte that shares its backing storage with s.
// No copy is made — the returned slice is a view of the string's bytes.
//
// Callers MUST treat the result as read-only. Writing through the slice mutates
// the underlying string memory, breaking the Go string immutability invariant
// and every other reader of that memory (interned constants, map keys, cached
// interfaces). Use only where the []byte is passed to a consumer known to read
// but not modify (marshallers, hash functions, io.Writer.Write, etc.).
func UnsafeStringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// UnsafeBytesToString returns a string that shares its backing storage with b.
// No copy is made — the returned string is a view of the slice's bytes.
//
// Callers MUST ensure b is not mutated for the lifetime of the returned string.
// If the underlying array is modified, the string's contents change silently —
// violating the immutability contract every other reader relies on.
func UnsafeBytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
