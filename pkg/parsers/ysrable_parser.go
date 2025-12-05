package parsers

import (
	"github.com/transferia/transferia/pkg/abstract"
)

// This parser wrapper should be used if parser uses Yandex Schema Registry (YSR) Namespace ID
// In order to check that user has permission on specified Namespace ID when
// creating/updating endpoint with such parser
type YSRableParser struct {
	Parser
	ysrNamespaceID string
}

func (p *YSRableParser) Unwrap() Parser {
	return p.Parser
}

func (p *YSRableParser) YSRNamespaceID() string {
	return p.ysrNamespaceID
}

func (p *YSRableParser) Do(msg Message, partition abstract.Partition) []abstract.ChangeItem {
	return p.Parser.Do(msg, partition)
}

func (p *YSRableParser) DoBatch(batch MessageBatch) []abstract.ChangeItem {
	return p.Parser.DoBatch(batch)
}

func WithYSRNamespaceIDs(parser Parser, ysrNamespaceID string) Parser {
	return &YSRableParser{
		Parser:         parser,
		ysrNamespaceID: ysrNamespaceID,
	}
}
