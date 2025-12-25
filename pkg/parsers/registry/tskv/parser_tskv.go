package tskv

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/parsers/resources"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func newParserTSKVLb(in *ParserConfigTSKVLb, sniff bool, logger log.Logger) (*generic.GenericParserConfig, []abstract.ColSchema, resources.AbstractResources, error) {
	resourcesObj, err := resources.NewResources(logger, []string{in.SchemaResourceName})
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to get resources, err: %w", err)
	}

	finalSchema, err := parsers.SchemaByFieldsAndResource(logger, resourcesObj, in.Fields, in.SchemaResourceName)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to get schema by fields & resource, err: %w", err)
	}

	return &generic.GenericParserConfig{
		Format:             "tskv",
		SchemaResourceName: in.SchemaResourceName,
		Fields:             in.Fields,
		AuxOpts: generic.AuxParserOpts{
			Topic:                  "",
			AddDedupeKeys:          true,
			MarkDedupeKeysAsSystem: in.SkipSystemKeys,
			AddSystemColumns:       in.AddSystemCols,
			AddTopicColumn:         false,
			AddRest:                in.AddRest,
			TimeField:              in.TimeField,
			InferTimeZone:          false,
			NullKeysAllowed:        in.NullKeysAllowed,
			DropUnparsed:           in.DropUnparsed,
			MaskSecrets:            in.MaskSecrets,
			IgnoreColumnPaths:      in.IgnoreColumnPaths,
			TableSplitter:          in.TableSplitter,
			Sniff:                  sniff,
			UseNumbersInAny:        false,
			UnescapeStringValues:   in.UnescapeStringValues,
			UnpackBytesBase64:      false,
		},
	}, finalSchema, resourcesObj, nil
}

func newParserTSKVCommon(in *ParserConfigTSKVCommon, sniff bool, logger log.Logger) (*generic.GenericParserConfig, []abstract.ColSchema, resources.AbstractResources, error) {
	resourcesObj, err := resources.NewResources(logger, []string{in.SchemaResourceName})
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to get resources, err: %w", err)
	}

	finalSchema, err := parsers.SchemaByFieldsAndResource(logger, resourcesObj, in.Fields, in.SchemaResourceName)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to get schema by fields & resource, err: %w", err)
	}

	return &generic.GenericParserConfig{
		Format:             "tskv",
		SchemaResourceName: in.SchemaResourceName,
		Fields:             in.Fields,
		AuxOpts: generic.AuxParserOpts{
			Topic:                  "",
			AddDedupeKeys:          true,
			MarkDedupeKeysAsSystem: false,
			AddSystemColumns:       false,
			AddTopicColumn:         false,
			AddRest:                in.AddRest,
			TimeField:              nil,
			InferTimeZone:          false,
			NullKeysAllowed:        in.NullKeysAllowed,
			DropUnparsed:           false,
			MaskSecrets:            false,
			IgnoreColumnPaths:      false,
			TableSplitter:          nil,
			Sniff:                  sniff,
			UseNumbersInAny:        false,
			UnescapeStringValues:   in.UnescapeStringValues,
			UnpackBytesBase64:      false,
		},
	}, finalSchema, resourcesObj, nil
}

func NewParserTSKV(inWrapped interface{}, sniff bool, logger log.Logger, registry *stats.SourceStats) (parsers.Parser, error) {
	var genericParserConfig *generic.GenericParserConfig
	var finalSchema []abstract.ColSchema
	var resourcesObj resources.AbstractResources
	var err error

	switch in := inWrapped.(type) {
	case *ParserConfigTSKVCommon:
		genericParserConfig, finalSchema, resourcesObj, err = newParserTSKVCommon(in, sniff, logger)
		if err != nil {
			return nil, xerrors.Errorf("unable to make *impl.GenericParserConfig, err: %w", err)
		}
	case *ParserConfigTSKVLb:
		genericParserConfig, finalSchema, resourcesObj, err = newParserTSKVLb(in, sniff, logger)
		if err != nil {
			return nil, xerrors.Errorf("unable to make *impl.GenericParserConfig, err: %w", err)
		}
	default:
		return nil, xerrors.Errorf("unknown parserConfig type: %T", inWrapped)
	}

	parserImpl := generic.NewGenericParser(genericParserConfig, finalSchema, logger, registry)

	return parsers.WithResource(parserImpl, resourcesObj), nil
}

func init() {
	parsers.Register(
		NewParserTSKV,
		[]parsers.AbstractParserConfig{new(ParserConfigTSKVCommon), new(ParserConfigTSKVLb)},
	)
}
