package connection

import (
	"github.com/nats-io/nats.go/jetstream"
)

/*
Config represents the overall configuration required to initialise a NATS Jetstream source
*/
type Config struct {
	Connection *ConnectionConfig
	// StreamIngestionConfigs contains a list of stream configurations, each specifying its own subjects and consumer behavior.
	StreamIngestionConfigs []*StreamIngestionConfig
}

/*
StreamIngestionConfig holds the configurations regarding stream and their subjects.
In NATS JetStream, a stream acts as a persistent storage layer for messages published to specific subjects.
A stream is bound to one or more subjects. This means each subject / group of subjects
within the stream can have a different schema, and would likely be ingested in a different target.
*/
type StreamIngestionConfig struct {
	Stream                  string
	SubjectIngestionConfigs []*SubjectIngestionConfig
}

/*
SubjectIngestionConfig holds the configuration regarding a group of subject within in a stream.
A consumer in NATS JetStream is a subscription mechanism that allows clients to retrieve messages from a stream.
List of filter subjects from which messages have to be consumed  are a part of standard ConsumerConfig provided by NATS library.
ParserConfig defines the schema of the messages in these subjects and how to parse them.
*/
type SubjectIngestionConfig struct {
	TableName      string
	ConsumerConfig *jetstream.ConsumerConfig
	ParserConfig   map[string]interface{}
}
