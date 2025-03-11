package protobuf

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferria/transferria/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type ProtoConfigurable interface {
	ToProtoParserConfig(log.Logger) (*protoparser.ProtoParserConfig, error)
}

func NewParserProto(inWrapped interface{}, _ bool, logger log.Logger, metrics *stats.SourceStats) (parsers.Parser, error) {
	in, ok := inWrapped.(ProtoConfigurable)
	if !ok {
		return nil, xerrors.Errorf("can't extract proto parser config from provided input: %v", inWrapped)
	}

	conf, err := in.ToProtoParserConfig(logger)
	if err != nil {
		return nil, xerrors.Errorf("error creating parser config: %v", err)
	}

	parser, err := protoparser.NewProtoParser(conf, metrics)
	if err != nil {
		return nil, xerrors.Errorf("error creating parser from config: %v", err)
	}

	return parser, nil
}

func init() {
	parsers.Register(
		NewParserProto,
		[]parsers.AbstractParserConfig{new(ParserConfigProtoLb), new(ParserConfigProtoCommon)},
	)
}
