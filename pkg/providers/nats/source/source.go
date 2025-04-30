package source

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/transferia/transferia/pkg/providers/nats/connection"
	"github.com/transferia/transferia/pkg/providers/nats/reader"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const ProviderType = abstract.ProviderType("nats")

/*
NatsSource is the main struct that holds the nats source initialisation configuration.
It implements the source interface, stores the active nats client connection and all the readers responsible for
a group of subjects in a NATS stream.
*/
type NatsSource struct {
	Config  *connection.Config
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	errCh   chan error
	logger  log.Logger
	metrics *stats.SourceStats
	// A list of readers for different group of subjects
	readers []*reader.NatsReader
	// contains the actual nats client connection.
	conn *nats.Conn
	// for simplified jetstream api usage.
	jetStream jetstream.JetStream
}

var _ model.Source = (*NatsSource)(nil)

func (s *NatsSource) WithDefaults() {

	if s.Config == nil {
		s.Config = &connection.Config{}
	}
	if s.Config.Connection == nil {
		s.Config.Connection = &connection.ConnectionConfig{
			NatsConnectionOptions: &connection.NatsConnectionOptions{
				Url:          nats.DefaultURL,
				MaxReconnect: 10,
			},
		}
	}
}

func (s *NatsSource) WithConfig(config *connection.Config) *NatsSource {
	s.Config = config
	return s
}

func (s *NatsSource) WithStreamIngestionConfig(streamsIngestionConfig []*connection.StreamIngestionConfig) *NatsSource {
	s.Config.StreamIngestionConfigs = streamsIngestionConfig
	return s
}

func (s *NatsSource) IsSource() {
}

func (s *NatsSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *NatsSource) Validate() error {

	// Validate all stream and subject configurations.
	for _, streamConfig := range s.Config.StreamIngestionConfigs {
		for _, subjectConfig := range streamConfig.SubjectIngestionConfigs {
			if subjectConfig.ParserConfig != nil {
				parserConfigStruct, err := parsers.ParserConfigMapToStruct(subjectConfig.ParserConfig)
				if err != nil {
					return xerrors.Errorf("unable to create new parser Config, err: %w", err)
				}
				if err = parserConfigStruct.Validate(); err != nil {
					return xerrors.Errorf("unable to validate new parser Config, err: %w", err)
				}
			}
		}
	}

	return nil
}

// New now creates a reader for every (stream, group of subjects) pair.
func New(config *connection.Config, logger log.Logger, registry metrics.Registry) (*NatsSource, error) {

	ctx, cancel := context.WithCancel(context.Background())

	source := &NatsSource{
		Config:  config,
		metrics: stats.NewSourceStats(registry),
		logger:  logger,
		cancel:  cancel,
		ctx:     ctx,
		errCh:   make(chan error, 1),
	}

	// Connect to NATS and obtain the JetStream context.
	conn, err := nats.Connect(config.Connection.NatsConnectionOptions.Url, nats.MaxReconnects(config.Connection.NatsConnectionOptions.MaxReconnect))
	if err != nil {
		return nil, xerrors.Errorf("error while connecting to nats: %w", err)
	}

	jetStream, err := jetstream.New(conn)
	if err != nil {
		return nil, xerrors.Errorf("error while creating new jetstream instance: %w", err)
	}

	// Create a reader for each stream/subject configuration.
	for _, streamConfig := range config.StreamIngestionConfigs {
		for _, subjectConfig := range streamConfig.SubjectIngestionConfigs {

			natsReader, err := reader.New(ctx, jetStream, streamConfig.Stream, subjectConfig, logger, source.metrics)
			if err != nil {
				conn.Close()

				return nil, xerrors.Errorf("error while creating natsReader for stream %s, table %s: %w",
					streamConfig.Stream, subjectConfig.TableName, err)
			}

			source.readers = append(source.readers, natsReader)
		}
	}

	if len(source.readers) == 0 {
		conn.Close()
		return nil, xerrors.Errorf("no valid stream and subject configuration provided")
	}

	source.conn = conn
	source.jetStream = jetStream

	return source, nil
}

func (s *NatsSource) parse(r *reader.NatsReader, buffer []jetstream.Msg) []abstract.ChangeItem {

	var data []abstract.ChangeItem

	for _, msg := range buffer {
		metadata, _ := msg.Metadata()
		data = append(data,
			abstract.ChangeItem{
				Kind:  abstract.InsertKind,
				Table: r.SubjectIngestionConfig.TableName,
				ColumnValues: []interface{}{
					msg.Subject(),            // shard (subject)
					metadata.Sequence.Stream, // offset
					metadata.Timestamp,       // commit time
					msg.Data(),               // data payload
				},
				Size: abstract.RawEventSize(uint64(len(msg.Data()))),
				LSN:  metadata.Sequence.Stream,
			})
	}

	if r.Parser != nil {
		st := time.Now()
		var converted []abstract.ChangeItem

		for _, row := range data {
			ci, part := s.changeItemAsMessage(row, r.SubjectIngestionConfig.TableName)
			converted = append(converted, r.Parser.Do(ci, part)...)
		}

		s.logger.Infof("convert done in %v, %v rows -> %v rows", time.Since(st), len(data), len(converted))
		data = converted
		s.metrics.DecodeTime.RecordDuration(time.Since(st))
	}

	s.metrics.ChangeItems.Add(int64(len(data)))

	for _, ci := range data {
		if ci.IsRowEvent() {
			s.metrics.Parsed.Inc()
		}
	}

	return data
}

func (s *NatsSource) ack(r *reader.NatsReader) func(data []jetstream.Msg, pushSt time.Time, err error) {

	return func(data []jetstream.Msg, pushSt time.Time, err error) {
		if err := r.AckMessages(data); err != nil {
			util.Send(s.ctx, s.errCh, err)
			return
		}

		s.logger.Trace(fmt.Sprintf("Ack messages done in %v", time.Since(pushSt)))
		s.metrics.PushTime.RecordDuration(time.Since(pushSt))
	}

}

func (s *NatsSource) Run(sink abstract.AsyncSink) error {

	eg := new(errgroup.Group)
	for _, natsReader := range s.readers {
		localReader := natsReader

		eg.Go(func() error {
			// Each reader gets its own parseQueue with closures referencing that reader.
			parseWrapper := func(buffer []jetstream.Msg) []abstract.ChangeItem {
				return s.parse(localReader, buffer)
			}
			parseQ := parsequeue.NewWaitable(s.logger, 10, sink, parseWrapper, s.ack(localReader))
			defer parseQ.Close()
			// Read loop for this reader.
			for {
				// Check for shutdown signals.
				select {
				case <-s.ctx.Done():
					return nil
				case <-s.errCh:
					s.cancel()
					return nil
				default:
				}
				// Fetch messages from the reader.
				messageBatch, err := localReader.Fetch()
				errTimeoutOrNoMsgs := errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages)
				if err != nil && !errTimeoutOrNoMsgs {
					util.Send(s.ctx, s.errCh, err)
					return nil
				}

				if errTimeoutOrNoMsgs {
					s.logger.Info("no input from nats")
					continue
				}

				// messageBatch has a channel, from which messages are read one after another.
				// since we use the same parse queue for both parsing and acking, it is important to
				// store messages in a separate array so that it could be acked later.
				messages := make([]jetstream.Msg, 0)
				for msg := range messageBatch.Messages() {
					messages = append(messages, msg)
				}

				if len(messages) == 0 {
					continue
				}

				// Push the fetched messages to the parse queue.
				if err := parseQ.Add(messages); err != nil {
					util.Send(s.ctx, s.errCh, err)
					return nil
				}

			}
		})

	}
	if err := eg.Wait(); err != nil {
		s.logger.Error("error in run loop: ", log.Error(err))
	}
	s.Stop()
	return nil
}

func (s *NatsSource) Stop() {

	s.once.Do(func() {
		s.cancel()
		_ = s.conn.Drain()
	})
}

func (s *NatsSource) changeItemAsMessage(ci abstract.ChangeItem, tableName string) (parsers.Message, abstract.Partition) {

	seqNo := ci.ColumnValues[1].(uint64)
	wTime := ci.ColumnValues[2].(time.Time)

	return parsers.Message{
			Offset:     ci.LSN,
			SeqNo:      seqNo,
			Key:        nil,
			CreateTime: time.Unix(0, int64(ci.CommitTime)),
			WriteTime:  wTime,
			Value:      ci.ColumnValues[3].([]byte),
		}, abstract.Partition{
			Topic: tableName,
		}
}
