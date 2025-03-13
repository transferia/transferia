package reader

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/nats/connection"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"time"
)

/*
NatsReader is an internal struct that deals with a consumer using simplified jetstream api.
It is responsible for fetching and acking messages using consumer.
It also stores the subject configuration for which a consumer is created.
It also holds the Parser which is used to parse the incoming nats message payload from a []byte to ChangeItem for targets.
*/
type NatsReader struct {
	SubjectIngestionConfig *connection.SubjectIngestionConfig
	Stream                 string
	Parser                 parsers.Parser
	Consumer               jetstream.Consumer
}

func New(ctx context.Context, jetStream jetstream.JetStream, stream string, subjectConfig *connection.SubjectIngestionConfig, logger log.Logger, stats *stats.SourceStats) (*NatsReader, error) {

	consumer, err := jetStream.CreateOrUpdateConsumer(ctx, stream, *subjectConfig.ConsumerConfig)
	if err != nil {
		return nil, xerrors.Errorf("unable to create create or update consumer, err: %w", err)
	}

	var parser parsers.Parser
	if subjectConfig.ParserConfig != nil {
		parser, err = parsers.NewParserFromMap(subjectConfig.ParserConfig, false, logger, stats)
		if err != nil {
			return nil, xerrors.Errorf("unable to make Parser, err: %w", err)
		}
	}

	return &NatsReader{
		Stream:                 stream,
		SubjectIngestionConfig: subjectConfig,
		Parser:                 parser,
		Consumer:               consumer,
	}, nil

}

// AckMessages is responsible for acking messages based on ack policy provided in consumer configuration while initialising
// nats source.
func (r *NatsReader) AckMessages(msgs []jetstream.Msg) error {

	switch r.SubjectIngestionConfig.ConsumerConfig.AckPolicy {
	case jetstream.AckExplicitPolicy:
		for _, msg := range msgs {
			err := msg.Ack()
			if err != nil {
				return err
			}
		}
	case jetstream.AckAllPolicy:
		return msgs[len(msgs)-1].Ack()
	case jetstream.AckNonePolicy:
		return nil
	default:
	}

	return xerrors.New(fmt.Sprintf("unsupported ack policy: %s", r.SubjectIngestionConfig.ConsumerConfig.AckPolicy.String()))

}

func (r *NatsReader) Fetch() (jetstream.MessageBatch, error) {
	msgs, err := r.Consumer.Fetch(r.SubjectIngestionConfig.ConsumerConfig.MaxRequestBatch,
		jetstream.FetchMaxWait(10*time.Second))
	return msgs, err
}
