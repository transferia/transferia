//go:build !disable_kafka_provider

package writer

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"go.ytsaurus.tech/library/go/core/log"
)

// how to generate mock from 'AbstractWriter' and 'AbstractWriterFactory' interfaces:
// > export GO111MODULE=on && ya tool mockgen -source ./abstract.go -package writer -destination ./writer_mock.go

type AbstractWriter interface {
	WriteMessages(ctx context.Context, lgr log.Logger, topicName string, currMessages []serializer.SerializedMessage) error
	Close() error
}

type AbstractWriterFactory interface {
	BuildWriter(brokers []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config, topicConfig [][2]string, batchBytes int64, dial func(ctx context.Context, network string, address string) (net.Conn, error)) AbstractWriter
}
