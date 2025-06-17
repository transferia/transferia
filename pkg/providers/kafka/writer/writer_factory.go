//go:build !disable_kafka_provider

package writer

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"go.ytsaurus.tech/library/go/core/log"
)

type WriterFactory struct {
	lgr log.Logger
}

func NewWriterFactory(lgr log.Logger) *WriterFactory {
	return &WriterFactory{
		lgr: lgr,
	}
}

func (c *WriterFactory) BuildWriter(brokers []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config, topicConfig [][2]string, batchBytes int64, dial func(ctx context.Context, network string, address string) (net.Conn, error)) AbstractWriter {
	return NewWriter(brokers, compression, saslMechanism, tlsConfig, topicConfig, batchBytes, dial)
}
