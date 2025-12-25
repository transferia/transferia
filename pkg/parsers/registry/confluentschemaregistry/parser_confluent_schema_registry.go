package confluentschemaregistry

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	conflueentschemaregistryengine "github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/engine"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserConfluentSchemaRegistry(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	var srURL, username, password, namespaceID, tlsFile string
	var generateUpdates bool
	switch in := inWrapped.(type) {
	case *ParserConfigConfluentSchemaRegistryCommon:
		srURL, username, password = in.SchemaRegistryURL, in.Username, in.Password
		tlsFile, namespaceID = in.TLSFile, in.NamespaceID
		generateUpdates = in.IsGenerateUpdates
	case *ParserConfigConfluentSchemaRegistryLb:
		srURL, username, password = in.SchemaRegistryURL, in.Username, in.Password
		tlsFile, namespaceID = in.TLSFile, in.NamespaceID
		generateUpdates = in.IsGenerateUpdates
	default:
		return nil, xerrors.Errorf("unknown parser config type '%T'", inWrapped)
	}
	if namespaceID != "" {
		return parsers.WithYSRNamespaceIDs(func() (parsers.Parser, abstract.Expirer, error) {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
			}
			parserImpl := conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(params.URL, tlsFile, params.Username, params.Password, generateUpdates, false, logger)
			return parserImpl, &params, nil
		}, namespaceID, logger)

	}
	return conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(srURL, tlsFile, username, password, generateUpdates, false, logger), nil
}

func init() {
	parsers.Register(
		NewParserConfluentSchemaRegistry,
		[]parsers.AbstractParserConfig{new(ParserConfigConfluentSchemaRegistryCommon), new(ParserConfigConfluentSchemaRegistryLb)},
	)
}
