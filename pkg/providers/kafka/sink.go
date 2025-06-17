//go:build !disable_kafka_provider

package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/kafka/writer"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	queues "github.com/transferia/transferia/pkg/util/queues"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	pushTimeout = 5 * time.Minute
)

type sink struct {
	config     *KafkaDestination
	logger     log.Logger
	metrics    *stats.SinkerStats
	serializer serializer.Serializer
	writer     writer.AbstractWriter
}

func serializeKafkaMirror(input []abstract.ChangeItem) map[abstract.TablePartID][]serializer.SerializedMessage {
	tableToMessages := make(map[abstract.TablePartID][]serializer.SerializedMessage)
	arr := make([]serializer.SerializedMessage, 0, len(input))
	for _, changeItem := range input {
		arr = append(arr, serializer.SerializedMessage{
			Key:   GetKafkaRawMessageKey(&changeItem),
			Value: GetKafkaRawMessageData(&changeItem),
		})
	}
	if len(input) != 0 {
		fqtnWithPartID := input[0].TablePartID()
		tableToMessages[fqtnWithPartID] = arr
	}
	return tableToMessages
}

func (s *sink) Push(input []abstract.ChangeItem) error {
	start := time.Now()

	// serialize

	var tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage
	var err error
	if s.config.FormatSettings.Name == model.SerializationFormatLbMirror { // see comments to the function 'GroupAndSerializeLB'
		// 'id' here - sourceID
		tableToMessages, _, err = s.serializer.(*serializer.MirrorSerializer).GroupAndSerializeLB(input)
	} else {
		// 'id' here - fqtn()
		if IsKafkaRawMessage(input) {
			tableToMessages = serializeKafkaMirror(input)
		} else {
			tableToMessages, err = s.serializer.Serialize(input)
		}
	}
	if err != nil {
		return xerrors.Errorf("unable to serialize: %w", err)
	}
	serializer.LogBatchingStat(s.logger, input, tableToMessages, start)

	var pushTasks []abstract.TablePartID
	for currTablePartID := range tableToMessages {
		if currTablePartID.IsSystemTable() && !s.config.AddSystemTables {
			continue
		}
		pushTasks = append(pushTasks, currTablePartID)
	}
	// send

	startSending := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), pushTimeout)
	defer cancel()

	timings := queues.NewTimingsStatCollector()
	err = util.ParallelDoWithContextAbort(ctx, len(pushTasks), s.config.ParralelWriterCount, func(i int, ctx context.Context) error {
		currTablePartID := pushTasks[i]
		currMessages := tableToMessages[pushTasks[i]]
		timings.Started(currTablePartID)
		topicName := queues.GetTopicName(s.config.Topic, s.config.TopicPrefix, currTablePartID)

		if err := s.writer.WriteMessages(ctx, s.logger, topicName, currMessages); err != nil {
			switch t := err.(type) {
			case kafka.WriteErrors:
				return xerrors.Errorf("returned kafka.WriteErrors, err: %w", util.NewErrs(t...))
			case kafka.MessageTooLargeError:
				return xerrors.Errorf("message exceeded max message size (current BatchBytes: %d, len(key):%d, len(val):%d", s.config.BatchBytes, len(t.Message.Key), len(t.Message.Value))
			default:
				return xerrors.Errorf("unable to write messages, len(input): %d, tableID: %s, messages: %d : %w", len(input), currTablePartID.Fqtn(), len(currMessages), err)
			}
		}
		s.metrics.Table(currTablePartID.Fqtn(), "rows", len(currMessages))
		timings.Finished(currTablePartID)
		return nil
	})
	if err != nil {
		return xerrors.Errorf("unable to push messages: %w", err)
	}
	s.logger.Info("Sending sync timings stat", append([]log.Field{log.String("push_elapsed", time.Since(start).String()), log.String("sending_elapsed", time.Since(startSending).String())}, timings.GetResults()...)...)
	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return nil
}

func (s *sink) Close() error {
	if err := s.writer.Close(); err != nil {
		return xerrors.Errorf("failed to close part: %w", err)
	}
	return nil
}

func NewSinkImpl(cfg *KafkaDestination, registry metrics.Registry, lgr log.Logger, writerFactory writer.AbstractWriterFactory, isSnapshot bool) (abstract.Sinker, error) {
	brokers, err := ResolveBrokers(cfg.Connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
	}
	lgr.Info("resolved brokers", log.Any("brokers", brokers))
	cfg.Connection.Brokers = brokers
	mechanism, err := cfg.Auth.GetAuthMechanism()
	if err != nil {
		return nil, xerrors.Errorf("unable to define auth mechanism: %w", err)
	}
	tlsCfg, err := cfg.Connection.TLSConfig()
	if err != nil {
		return nil, xerrors.Errorf("unable to construct tls config: %w", err)
	}
	cfg.Auth.Password, err = ResolvePassword(cfg.Connection, cfg.Auth)
	if err != nil {
		return nil, xerrors.Errorf("unable to get password: %w", err)
	}
	_, err = queues.NewTopicDefinition(cfg.Topic, cfg.TopicPrefix)
	if err != nil {
		return nil, xerrors.Errorf("unable to validate topic settings: %w", err)
	}

	currFormat := cfg.FormatSettings
	if currFormat.Name == model.SerializationFormatDebezium {
		currFormat = serializer.MakeFormatSettingsWithTopicPrefix(currFormat, cfg.TopicPrefix, cfg.Topic)
	}

	currSerializer, err := serializer.New(currFormat, cfg.SaveTxOrder, false, isSnapshot, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create serializer: %w", err)
	}

	result := sink{
		config:     cfg,
		logger:     lgr,
		metrics:    stats.NewSinkerStats(registry),
		serializer: currSerializer,
		writer:     writerFactory.BuildWriter(cfg.Connection.Brokers, cfg.Compression.AsKafka(), mechanism, tlsCfg, topicConfigEntryToSlices(cfg.TopicConfigEntries), cfg.BatchBytes, cfg.DialFunc),
	}

	return &result, nil
}

func NewReplicationSink(cfg *KafkaDestination, registry metrics.Registry, lgr log.Logger) (abstract.Sinker, error) {
	return NewSinkImpl(cfg, registry, lgr, writer.NewWriterFactory(lgr), false)
}

func NewSnapshotSink(cfg *KafkaDestination, registry metrics.Registry, lgr log.Logger) (abstract.Sinker, error) {
	return NewSinkImpl(cfg, registry, lgr, writer.NewWriterFactory(lgr), true)
}
