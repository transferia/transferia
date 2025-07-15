package util

import "hash/crc32"

func CRC32FromString(in string) uint32 {
	return crc32.Checksum([]byte(in), crc32.IEEETable)
}
