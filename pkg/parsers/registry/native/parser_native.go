package native

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type ParserNative struct {
	logger log.Logger
}

func (p *ParserNative) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	changeItems, err := abstract.UnmarshalChangeItems(msg.Value)
	if err != nil {
		p.logger.Debug("Unable to convert body to changeItems", log.Error(err), log.Any("body", util.Sample(string(msg.Value), 1*1024)))
	}
	return changeItems
}

func (p *ParserNative) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	for _, msg := range batch.Messages {
		changeItems, err := abstract.UnmarshalChangeItems(msg.Value)
		if err != nil {
			p.logger.Debug("Unable to convert body to changeItems", log.Error(err), log.Any("body", util.Sample(string(msg.Value), 1*1024)))
		}
		result = append(result, changeItems...)
	}
	return result
}

func NewParserNative(_ interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	parser := &ParserNative{
		logger: logger,
	}
	return parser, nil
}

func init() {
	parsers.Register(
		NewParserNative,
		[]parsers.AbstractParserConfig{new(ParserConfigNativeLb)},
	)
}
