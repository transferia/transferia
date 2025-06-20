//go:build !disable_kafka_provider

package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ AbstractWriter = (*Writer)(nil)

type Writer struct {
	brokers       []string
	saslMechanism sasl.Mechanism
	tlsConfig     *tls.Config
	topicConfig   [][2]string
	batchBytes    int64

	writersMutex sync.Mutex
	knownTopics  map[string]bool

	dial func(ctx context.Context, network string, address string) (net.Conn, error)

	rawKafkaWriter *kafka.Writer
}

func NewWriter(brokers []string, compression kafka.Compression, saslMechanism sasl.Mechanism, tlsConfig *tls.Config, topicConfig [][2]string, batchBytes int64, dial func(ctx context.Context, network string, address string) (net.Conn, error)) *Writer {
	rawKafkaWriter := &kafka.Writer{
		Addr:       kafka.TCP(brokers...),
		Balancer:   &kafka.Hash{},
		BatchBytes: batchBytes,
		Transport: &kafka.Transport{
			Dial: dial,
			TLS:  tlsConfig,
			SASL: saslMechanism,
		},

		Compression: compression,
	}
	return &Writer{
		brokers:       brokers,
		saslMechanism: saslMechanism,
		tlsConfig:     tlsConfig,
		topicConfig:   topicConfig,
		batchBytes:    batchBytes,

		writersMutex: sync.Mutex{},
		knownTopics:  make(map[string]bool),

		dial: dial,

		rawKafkaWriter: rawKafkaWriter,
	}
}

func (w *Writer) ensureTopicExists(lgr log.Logger, topic string) error {
	w.writersMutex.Lock()
	defer w.writersMutex.Unlock()
	writerID := fmt.Sprintf("topic:%v", topic)
	if w.knownTopics[writerID] {
		return nil
	}
	kafkaClient, err := client.NewClient(w.brokers, w.saslMechanism, w.tlsConfig, w.dial)
	if err != nil {
		return xerrors.Errorf("unable to create kafka client, err: %w", err)
	}
	if err := kafkaClient.CreateTopicIfNotExist(lgr, topic, w.topicConfig); err != nil {
		return xerrors.Errorf("unable to create topic, broker: %s, topic: %s, err: %w", w.brokers, topic, err)
	}
	w.knownTopics[writerID] = true
	return nil
}

func (w *Writer) WriteMessages(ctx context.Context, lgr log.Logger, topicName string, currMessages []serializer.SerializedMessage) error {
	err := w.ensureTopicExists(lgr, topicName)
	if err != nil {
		return xerrors.Errorf("unable to ensureTopicExists, topicName: %s, err: %w", topicName, err)
	}

	finalMsgs := make([]kafka.Message, 0, len(currMessages)) // bcs 'debezium' can generate 1..3 messages from one changeItem
	for _, msg := range currMessages {
		finalMsgs = append(finalMsgs, kafka.Message{Key: msg.Key, Value: msg.Value, Topic: topicName})
	}

	if err := w.rawKafkaWriter.WriteMessages(ctx, finalMsgs...); err != nil {
		switch t := err.(type) {
		case kafka.WriteErrors:
			return xerrors.Errorf("returned kafka.WriteErrors, err: %w", util.NewErrs(t...))
		case kafka.MessageTooLargeError:
			return xerrors.Errorf("message exceeded max message size (current BatchBytes: %d, len(key):%d, len(val):%d", w.batchBytes, len(t.Message.Key), len(t.Message.Value))
		default:
			return xerrors.Errorf("unable to write messages, topicName: %s, messages: %d : %w", topicName, len(currMessages), err)
		}
	}
	return nil
}

func (w *Writer) Close() error {
	return w.rawKafkaWriter.Close()
}
