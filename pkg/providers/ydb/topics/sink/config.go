package topicsink

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type Config struct {
	Topic            string
	TopicPrefix      string
	CompressionCodec CompressionCodec
	FormatSettings   model.SerializationFormat

	AddSystemTables bool
	SaveTxOrder     bool

	Endpoint    string
	Database    string
	Shard       string
	Credentials ydb.TokenCredentials

	TLS         model.TLSMode
	RootCAFiles []string
}

type CompressionCodec string

const (
	CompressionCodecUnspecified CompressionCodec = ""
	CompressionCodecRaw         CompressionCodec = "raw"
	CompressionCodecGzip        CompressionCodec = "gzip"
	CompressionCodecZstd        CompressionCodec = "zstd"
)

func (e CompressionCodec) ToTopicTypesCodec() topictypes.Codec {
	switch e {
	case CompressionCodecGzip:
		return topictypes.CodecGzip
	case CompressionCodecZstd:
		return topictypes.CodecZstd
	default:
		return topictypes.CodecRaw
	}
}
