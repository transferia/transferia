package writer

import (
	"context"
	"crypto/tls"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	serializer "github.com/transferria/transferria/pkg/serializer/queue"
	"go.ytsaurus.tech/library/go/core/log"
)

// how to generate mock from 'AbstractWriter' and 'AbstractWriterFactory' interfaces:
// > export GO111MODULE=on && ya tool mockgen -source ./abstract.go -package writer -destination ./writer_mock.go

type AbstractWriter interface {
	WriteMessages(ctx context.Context, lgr log.Logger, topicName string, currMessages []serializer.SerializedMessage) error
	Close() error
}

type AbstractWriterFactory interface {
	BuildWriter(brokers []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config, topicConfig [][2]string, batchBytes int64) AbstractWriter
}
