package protoparser

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
	"github.com/transferia/transferia/pkg/stats"
)

var _ parsers.ParserBuilder = (*lazyProtoParserBuilder)(nil)

type lazyProtoParserBuilder struct {
	baseParser *ProtoParser
}

func NewLazyProtoParserBuilder(cfg *ProtoParserConfig, metrics *stats.SourceStats) (parsers.ParserBuilder, error) {
	parser, err := NewProtoParser(cfg, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct base parser: %w", err)
	}
	return &lazyProtoParserBuilder{baseParser: parser}, nil
}

func (f *lazyProtoParserBuilder) BuildBaseParser() parsers.Parser {
	return f.baseParser
}

func (f *lazyProtoParserBuilder) BuildLazyParser(msg parsers.Message, partition abstract.Partition) (parsers.LazyParser, error) {
	cfg := f.baseParser.cfg
	sc, err := protoscanner.NewProtoScanner(cfg.ProtoScannerType, cfg.LineSplitter, msg.Value, cfg.ScannerMessageDesc)
	if err != nil {
		return nil, xerrors.Errorf("error creating scanner: %v", err)
	}
	return &lazyProtoParser{
		parser:     f.baseParser,
		sc:         sc,
		iterSt:     NewIterState(msg, partition),
		partition:  &partition,
		rawMessage: &msg,
	}, nil
}
