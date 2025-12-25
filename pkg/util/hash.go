package util

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"sync"
)

var (
	hasher   *HasherSha256
	hasherMu = sync.Mutex{}
)

func Hash(content string) string {
	h := md5.New()
	_, _ = fmt.Fprint(h, content)
	return hex.EncodeToString(h.Sum(nil))
}

func HashSha256(data []byte) string {
	hasherMu.Lock()
	defer hasherMu.Unlock()
	if hasher == nil {
		hasher = NewHasherSha256()
	}
	return hex.EncodeToString(hasher.Sum(data))
}

// use hasher with once allocated slice [sha256.Size]byte for result instead of sha256.Sum256() function to avoid extra memory allocations
type HasherSha256 struct {
	hasher hash.Hash
	result [sha256.Size]byte
}

func (h *HasherSha256) Sum(data []byte) []byte {
	h.hasher.Reset()
	h.hasher.Write(data)
	return h.hasher.Sum(h.result[:0])
}

func NewHasherSha256() *HasherSha256 {
	return &HasherSha256{
		hasher: sha256.New(),
		result: [sha256.Size]byte{},
	}
}
