package engine

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type CloudLoggingImpl struct {
	parser parsers.Parser
}

func (p *CloudLoggingImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	return p.parser.Do(msg, partition)
}

func (p *CloudLoggingImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, 1000)
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func makeColSchemaImpl(name, type_ string, pkey bool) abstract.ColSchema {
	return abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         name,
		ColumnName:   name,
		DataType:     type_,
		PrimaryKey:   pkey,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func makeColSchema(name, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, type_, false)
}

func makeColSchemaPK(name, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, type_, true)
}

func getFields() []abstract.ColSchema {
	result := make([]abstract.ColSchema, 0)
	result = append(result, makeColSchemaPK("timestamp", "datetime"))
	result = append(result, makeColSchemaPK("uid", "string"))
	result = append(result, makeColSchema("resource", "any"))
	result = append(result, makeColSchema("ingestedAt", "datetime"))
	result = append(result, makeColSchema("savedAt", "datetime"))
	result = append(result, makeColSchema("level", "string"))
	result = append(result, makeColSchema("message", "string"))
	result = append(result, makeColSchema("jsonPayload", "any"))
	result = append(result, makeColSchema("streamName", "string"))
	return result
}

func NewCloudLoggingImpl(sniff bool, logger log.Logger, registry *stats.SourceStats) *CloudLoggingImpl {
	fields := getFields()

	config := &generic.GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: generic.AuxParserOpts{
			Topic:                  "",
			AddDedupeKeys:          false,
			MarkDedupeKeysAsSystem: false,
			AddSystemColumns:       false,
			AddTopicColumn:         false,
			AddRest:                false,
			TimeField:              nil,
			InferTimeZone:          true,
			NullKeysAllowed:        false,
			DropUnparsed:           false,
			MaskSecrets:            false,
			IgnoreColumnPaths:      false,
			TableSplitter:          nil,
			Sniff:                  sniff,
			UseNumbersInAny:        false,
			UnescapeStringValues:   false,
			UnpackBytesBase64:      false,
		},
	}

	parserImpl := generic.NewGenericParser(config, fields, logger, registry)

	return &CloudLoggingImpl{
		parser: parserImpl,
	}
}
