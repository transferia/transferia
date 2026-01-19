package util

import "github.com/transferia/transferia/library/go/core/xerrors"

func ZigzagVarIntDecode(data []byte) (int64, int, error) {
	if len(data) == 0 {
		return 0, 0, xerrors.New("empty input")
	}

	var val uint64

	var i int
	for i = 0; i < len(data); i++ {
		if i == 10 {
			return 0, 0, xerrors.New("varint too long or incomplete")
		}

		b := data[i]
		val |= uint64(b&0x7F) << (i * 7)
		if b < 0x80 {
			break
		}
	}

	return ZigzagDecode(val), i + 1, nil
}

// ZigzagDecode - decodes values, encoded with zigzag-encoding
//
// https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding
//
// examples of zigzag-encogind (value -> zigzag encoded value):
//
// 0 -> 0
// -1 -> 1
// 1 -> 2
// -2 -> 3
// ...
//
// Also see tests for additional examples
func ZigzagDecode(n uint64) int64 {
	if n%2 == 0 {
		return int64(n / 2)
	}
	return -int64(n/2 + 1)
}
