//go:build !disable_kafka_provider

package kafka

import "github.com/segmentio/kafka-go"

type Encoding string

func (e Encoding) AsKafka() kafka.Compression {
	switch e {
	case GzipEncoding:
		return kafka.Gzip
	case SnappyEncoding:
		return kafka.Snappy
	case LZ4Encoding:
		return kafka.Lz4
	case ZstdEncoding:
		return kafka.Zstd
	default:
		return 0 // means disabled
	}
}

const (
	NoEncoding     = Encoding("UNCOMPRESSED")
	GzipEncoding   = Encoding("GZIP")
	SnappyEncoding = Encoding("SNAPPY")
	LZ4Encoding    = Encoding("LZ4")
	ZstdEncoding   = Encoding("ZSTD")
)
