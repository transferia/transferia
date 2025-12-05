package debezium

import (
	"runtime"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	debeziumengine "github.com/transferia/transferia/pkg/parsers/registry/debezium/engine"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserDebezium(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	var client *confluent.SchemaRegistryClient = nil
	var err error
	var namespaceID string
	switch in := inWrapped.(type) {
	case *ParserConfigDebeziumLb:
		srURL, username, password := in.SchemaRegistryURL, in.Username, in.Password
		namespaceID = in.NamespaceID
		if namespaceID != "" {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
			}
			srURL, username, password = params.URL, params.Username, params.Password
		}
		if srURL == "" {
			break
		}
		client, err = confluent.NewSchemaRegistryClientWithTransport(srURL, in.TLSFile, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		client.SetCredentials(username, password)
	case *ParserConfigDebeziumCommon:
		srURL, username, password := in.SchemaRegistryURL, in.Username, in.Password
		namespaceID = in.NamespaceID
		if namespaceID != "" {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
			}
			srURL, username, password = params.URL, params.Username, params.Password
		}
		if srURL == "" {
			break
		}
		client, err = confluent.NewSchemaRegistryClientWithTransport(srURL, in.TLSFile, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		client.SetCredentials(username, password)
	}

	parserImpl := debeziumengine.NewDebeziumImpl(logger, client, uint64(runtime.NumCPU()*4))
	if namespaceID != "" {
		return parsers.WithYSRNamespaceIDs(parserImpl, namespaceID), nil
	}
	return parserImpl, nil
}

func init() {
	parsers.Register(
		NewParserDebezium,
		[]parsers.AbstractParserConfig{new(ParserConfigDebeziumCommon), new(ParserConfigDebeziumLb)},
	)
}
