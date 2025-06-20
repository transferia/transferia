//go:build !disable_clickhouse_provider

package clickhouse

import "bytes"

type BufWithPos struct {
	bytes.Buffer
	pos int
}

func (b *BufWithPos) RememberPos() {
	b.pos = len(b.Bytes())
}

func (b *BufWithPos) BufFromRememberedPos() []byte {
	result := b.Bytes()[b.pos:]
	b.pos = -1
	return result
}

func NewBufWithPos() *BufWithPos {
	result := new(BufWithPos)
	result.pos = -1
	return result
}
