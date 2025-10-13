package logbroker

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	queues "github.com/transferia/transferia/pkg/util/queues"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

// writerQueueLenSize is necessary to specify the required limit on the number of sending messages.
// This is a big enough value for sending light messages. For example, if the messages
// weigh 100 bytes each, then 10 Mb will be written at a time
const writerQueueLenSize = 100000

type cancelableWriter interface {
	Write(ctx context.Context, messages ...topicwriter.Message) error
	Close(ctx context.Context) error
}

type sink struct {
	config     *LbDestination
	logger     log.Logger
	metrics    *stats.SinkerStats
	serializer serializer.Serializer

	// shard string - became part of Key
	//
	// Logbroker has 'writer session' entity, it's identified by Key.
	// At every moment of time, exists not more than one unique writer session.
	// Every Key corresponds one concrete partition number.
	// Map [hash(Key)->partition_number] is stored on lb-side forever.
	// So, it doesn't matter which string is in the 'shard' parameter - it needed only for hash generation.
	//
	// Q: Why default 'shard' value is transferID?
	// A: For users, when >1 transfers write in one topic - transferID as 'shard' supports this case out-of-the-box
	//     ('consolidation' of data from many sources into one topic case)
	//
	// Q: When someone may want set 'shard' parameter?
	// A: Every time, when lb-topic changed the number of partitions - to use all partitions,
	//     you need set new 'sharding policy' manually. It's possible only by setting 'shard' value.
	//
	// Beware, when you are changing the 'shard' parameter - it's resharding,
	// on this moment broken guarantee of consistency 'one tableName is into one partition'
	// So, recommended to do it carefully - for example, after consumer read all available data.
	shard string

	driver *ydb.Driver

	// writers - map: groupID -> writer
	// groupID is fqtn() for non-mirroring, and sourceID for mirroring
	// groupID further became sourceID
	// we need it for cases, when every
	writers *util.ConcurrentMap[string, cancelableWriter]
}

func (s *sink) Push(inputRaw []abstract.ChangeItem) error {
	start := time.Now()

	defer s.handleResetWorkers(inputRaw)
	input := s.getInputWithoutSynchronizeEvent(inputRaw)

	// serialize

	startSerialization := time.Now()
	var tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage
	var extras map[abstract.TablePartID]map[string]string = nil
	var err error
	perTableMetrics := true
	if s.config.FormatSettings.Name == model.SerializationFormatLbMirror { // see comments to the function 'GroupAndSerializeLB'
		// 'id' here - sourceID
		tableToMessages, extras, err = s.serializer.(*serializer.MirrorSerializer).GroupAndSerializeLB(input)
		perTableMetrics = false
	} else {
		// 'id' here - fqtn()
		tableToMessages, err = s.serializer.Serialize(input)
	}
	if err != nil {
		return xerrors.Errorf("unable to serialize: %w", err)
	}
	serializer.LogBatchingStat(s.logger, input, tableToMessages, startSerialization)

	// send asynchronous

	startSending := time.Now()

	timings := queues.NewTimingsStatCollector()

	err = s.sendSerializedMessages(timings, tableToMessages, extras)
	if err != nil {
		return xerrors.Errorf("sendSerializedMessages returned error, err: %w", err)
	}

	// handle metrics & logging
	if perTableMetrics {
		for groupID, currGroup := range tableToMessages {
			s.metrics.Table(groupID.Fqtn(), "rows", len(currGroup))
		}
	}

	s.logger.Info("Sending async timings stat", append([]log.Field{log.String("push_elapsed",
		time.Since(start).String()), log.String("sending_elapsed", time.Since(startSending).String())}, timings.GetResults()...)...)
	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return nil
}

func (s *sink) Close() error {
	err := s.closeWriters()
	if err != nil {
		return xerrors.Errorf("unable to close writers: %w", err)
	}
	if err := s.driver.Close(context.Background()); err != nil {
		return xerrors.Errorf("unable to close driver: %w", err)
	}
	return nil
}

func (s *sink) handleResetWorkers(input []abstract.ChangeItem) {
	if len(input) != 0 {
		lastIndex := len(input) - 1
		if !input[lastIndex].IsRowEvent() && !input[lastIndex].IsTxDone() {
			s.logger.Info("found non-row (and non-tx-done) event - reset writers")
			err := s.closeWriters()
			if err != nil {
				s.logger.Errorf("unable to close writers: %s", err)
			}
		}
	}
}

func (s *sink) getInputWithoutSynchronizeEvent(input []abstract.ChangeItem) []abstract.ChangeItem {
	if len(input) != 0 {
		lastIndex := len(input) - 1
		if input[lastIndex].Kind == abstract.SynchronizeKind {
			return input[0:lastIndex]
		}
	}
	return input
}

func (s *sink) sendSerializedMessages(
	timings *queues.TimingsStatCollector,
	tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage,
	extras map[abstract.TablePartID]map[string]string,
) error {
	tablePartIDs := make([]abstract.TablePartID, 0)
	for currTablePartID := range tableToMessages {
		if currTablePartID.IsSystemTable() && !s.config.AddSystemTables {
			continue
		}
		topic := queues.GetTopicName(s.config.Topic, s.config.TopicPrefix, currTablePartID)
		_, err := s.findOrCreateWriter(currTablePartID.FqtnWithPartID(), topic, extras[currTablePartID])
		if err != nil {
			return xerrors.Errorf("unable to find or create writer, topic: %s, err: %w", topic, err)
		}
		tablePartIDs = append(tablePartIDs, currTablePartID)
	}

	sendMessages := func(i int, ctx context.Context) error {
		tablePartID := tablePartIDs[i]
		serializedMessages := tableToMessages[tablePartID]

		timings.Started(tablePartID)

		groupID := tablePartID.FqtnWithPartID()
		currTopic := queues.GetTopicName(s.config.Topic, s.config.TopicPrefix, tablePartID)
		currWriter, err := s.findOrCreateWriter(groupID, currTopic, extras[tablePartID])
		if err != nil {
			return xerrors.Errorf("unable to find or create writer, topic: %s, err: %w", currTopic, err)
		}

		timings.FoundWriter(tablePartID)

		const maxSizePerWrite = 60 * 1024 * 1024
		messagesSize, messageBatches := splitSerializedMessages(maxSizePerWrite, serializedMessages)

		for _, b := range messageBatches {
			if err := currWriter.Write(ctx, b...); err != nil {
				if err := s.deleteWriterByGroupID(groupID); err != nil {
					s.logger.Errorf("unable to close writer for table %s, err: %s", groupID, err)
				}
				s.logger.Error("Cannot write message to Logbroker", log.String("table", groupID), log.Error(err))
				return xerrors.Errorf("cannot write message from table %s to topic %s: %w", groupID, s.config.Topic, err)
			}
		}

		timings.Finished(tablePartID)

		s.logger.Infof(
			"sent %d messages (%s bytes) from table %s to topic '%s'",
			len(serializedMessages),
			format.SizeInt(messagesSize),
			tablePartID.Fqtn(),
			s.config.Topic,
		)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := util.ParallelDoWithContextAbort(ctx, len(tablePartIDs), 10, sendMessages); err != nil {
		return xerrors.Errorf("unable to push messages: %w", err)
	}

	return nil
}

func (s *sink) findOrCreateWriter(groupID, topic string, extras map[string]string) (cancelableWriter, error) {
	if conn, ok := s.writers.Get(groupID); ok {
		return conn, nil
	}

	sourceID := fmt.Sprintf("%v_%v", s.shard, groupID)
	writerOpts := []topicoptions.WriterOption{
		topicoptions.WithWriterProducerID(sourceID),
		topicoptions.WithWriterCodec(s.config.CompressionCodec.ToTopicTypesCodec()),
		topicoptions.WithWriterSessionMeta(extras),
		topicoptions.WithWriterStartTimeout(60 * time.Second),
	}

	// writer should be in temporary variable, and should be written in s.writers only after successes Init()
	writer, err := newWriter(s.driver, topic, writerOpts)
	if err != nil {
		return nil, xerrors.Errorf("unable to build writer: %w", err)
	}

	s.writers.Set(groupID, writer)

	return writer, nil
}

func (s *sink) deleteWriterByGroupID(groupID string) error {
	currWriter, ok := s.writers.Delete(groupID)
	if !ok {
		return xerrors.Errorf("unable to find writer for table: %s, impossible case", groupID)
	}
	return currWriter.Close(context.Background())
}

func (s *sink) closeWriters() error {
	errs := util.NewErrs()

	s.writers.Clear(func(mp map[string]cancelableWriter) {
		for _, wr := range mp {
			if err := wr.Close(context.Background()); err != nil && !xerrors.Is(err, context.Canceled) {
				errs = util.AppendErr(errs, xerrors.Errorf("failed to close Writer: %w", err))
			}
		}
	})

	if len(errs) > 0 {
		return errs
	}

	return nil
}

func splitSerializedMessages(maxSize int, serializedMessages []serializer.SerializedMessage) (int, [][]topicwriter.Message) {
	var totalMessagesSize, currentBatchSize int
	currentBatch := make([]topicwriter.Message, 0)
	messageBatches := make([][]topicwriter.Message, 0)
	for idx, currSerializedMessage := range serializedMessages {
		currentBatchSize += len(currSerializedMessage.Value)
		currentBatch = append(currentBatch, topicwriter.Message{Data: bytes.NewReader(currSerializedMessage.Value)})

		if currentBatchSize >= maxSize || idx == len(serializedMessages)-1 {
			totalMessagesSize += currentBatchSize
			messageBatches = append(messageBatches, currentBatch)

			currentBatchSize = 0
			currentBatch = make([]topicwriter.Message, 0)
		}
	}

	return totalMessagesSize, messageBatches
}

func newWriter(driver *ydb.Driver, topic string, opts []topicoptions.WriterOption) (cancelableWriter, error) {
	defaultOpts := []topicoptions.WriterOption{
		topicoptions.WithWriterStartTimeout(20 * time.Second),
		topicoptions.WithWriterMaxQueueLen(writerQueueLenSize),
		topicoptions.WithWriterWaitServerAck(true),
	}
	writerOpts := append(opts, defaultOpts...)
	writer, err := driver.Topic().StartWriter(topic, writerOpts...)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create topic writer: %w", err)
	}

	return writer, nil
}

func newDriver(cfg *LbDestination, lgr log.Logger) (*ydb.Driver, error) {
	isSecure := false
	opts := []ydb.Option{
		logadapter.WithTraces(lgr, trace.DetailsAll),
	}

	if cfg.Credentials != nil {
		opts = append(opts, ydb.WithCredentials(cfg.Credentials))
	}

	if cfg.TLS == EnabledTLS {
		isSecure = true
		tlsConfig, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("cannot init driver without tls: %w", err)
		}
		opts = append(opts, ydb.WithTLSConfig(tlsConfig))
	}

	driverCtx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	return ydb.Open(driverCtx, sugar.DSN(cfg.InstanceWithPort(), cfg.DB(), sugar.WithSecure(isSecure)), opts...)
}

func newSinkWithFactories(cfg *LbDestination, registry metrics.Registry, lgr log.Logger,
	transferID string, isSnapshot bool) (abstract.Sinker, error) {
	_, err := queues.NewTopicDefinition(cfg.Topic, cfg.TopicPrefix)
	if err != nil {
		return nil, xerrors.Errorf("unable to validate topic settings: %w", err)
	}

	currFormat := cfg.FormatSettings
	if cfg.FormatSettings.Name == model.SerializationFormatDebezium {
		currFormat = serializer.MakeFormatSettingsWithTopicPrefix(currFormat, cfg.TopicPrefix, cfg.Topic)
	}

	currSerializer, err := serializer.New(currFormat, cfg.SaveTxOrder, true, isSnapshot, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create serializer: %w", err)
	}

	driver, err := newDriver(cfg, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create driver, try to check DSN: %w", err)
	}

	sink := sink{
		config:     cfg,
		logger:     lgr,
		metrics:    stats.NewSinkerStats(registry),
		serializer: currSerializer,
		driver:     driver,
		writers:    util.NewConcurrentMap[string, cancelableWriter](),
		shard:      cfg.Shard,
	}

	if sink.shard == "" {
		sink.shard = transferID
	}

	return &sink, nil
}

func NewYDSSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger,
	transferID string) (abstract.Sinker, error) {
	return newSinkWithFactories(cfg, registry, lgr, transferID, false)
}

func NewReplicationSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return newSinkWithFactories(cfg, registry, lgr, transferID, false)
}

func NewSnapshotSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return newSinkWithFactories(cfg, registry, lgr, transferID, true)
}
