package confluentschemaregistry

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	conflueentschemaregistryengine "github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/engine"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserConfluentSchemaRegistry(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	switch in := inWrapped.(type) {
	case *ParserConfigConfluentSchemaRegistryCommon:
		srURL, username, password := in.SchemaRegistryURL, in.Username, in.Password
		namespaceID := in.NamespaceID
		if namespaceID != "" {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
			}
			srURL, username, password = params.URL, params.Username, params.Password
		}
		parserImpl := conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(srURL, in.TLSFile, username, password, in.IsGenerateUpdates, false, logger)
		if namespaceID != "" {
			return parsers.WithYSRNamespaceIDs(parserImpl, namespaceID), nil
		}
		return parserImpl, nil
	case *ParserConfigConfluentSchemaRegistryLb:
		srURL, username, password := in.SchemaRegistryURL, in.Username, in.Password
		namespaceID := in.NamespaceID
		if namespaceID != "" {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
			}
			srURL, username, password = params.URL, params.Username, params.Password
		}
		parserImpl := conflueentschemaregistryengine.NewConfluentSchemaRegistryImpl(srURL, in.TLSFile, username, password, in.IsGenerateUpdates, false, logger)
		if namespaceID != "" {
			return parsers.WithYSRNamespaceIDs(parserImpl, namespaceID), nil
		}
		return parserImpl, nil
	}
	return nil, nil
}

func init() {
	parsers.Register(
		NewParserConfluentSchemaRegistry,
		[]parsers.AbstractParserConfig{new(ParserConfigConfluentSchemaRegistryCommon), new(ParserConfigConfluentSchemaRegistryLb)},
	)
}
