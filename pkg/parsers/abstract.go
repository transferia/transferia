package parsers

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

// Message is struct describing incoming message
type Message struct {
	// Offset is server sequence of message in topic. Must be monotone growing.
	Offset uint64
	// Key is an uniq identifier of sequence
	Key []byte
	// Value actual data
	Value []byte
	// CreateTime when data was created on client (if presented)
	CreateTime time.Time
	// WriteTime when data was written to queue (if presented)
	WriteTime time.Time
	// Headers lables attached to read message
	Headers map[string]string
	// Deprecated: SeqNo is client set mark of message. Must be growing.
	// 	set it to 0 in new code, prefer using offset
	SeqNo uint64
}

// MessageBatch is group of messages.
type MessageBatch struct {
	Topic     string
	Partition uint32
	Messages  []Message
}

type Parser interface {
	Do(msg Message, partition abstract.Partition) []abstract.ChangeItem
	DoBatch(batch MessageBatch) []abstract.ChangeItem
}

// WrappedParser parser can be layered by wrapping them in extra layers.
// For wrapped parsers we should add extra method for extracting actual parser
type WrappedParser interface {
	Parser
	Unwrap() Parser
}

type AbstractParserConfig interface {
	IsNewParserConfig()
	IsAppendOnly() bool
	Validate() error
}

type YSRable interface {
	YSRNamespaceID() string
}

// now only one parser builder is supported
// /transfer_manager/go/pkg/parsers/registry/protobuf/protoparser/proto_parser_lazy_builder.go
type ParserBuilder interface {
	// BuildLazyParser prepares instance of LazyParser for provided data.
	BuildLazyParser(msg Message, partition abstract.Partition) (LazyParser, error)
	// BuildBaseParser builds a base parser for the provided message.
	BuildBaseParser() Parser
}

// LazyParser is a parser that can be used to parse messages in a streaming manner.
type LazyParser interface {
	// Next reads messages set by `Set` and returns next ChangeItem
	// in sequence, or nil if all items were already returned.
	Next() *abstract.ChangeItem
}
