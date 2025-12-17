package debezium

import (
	"runtime"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	debeziumengine "github.com/transferia/transferia/pkg/parsers/registry/debezium/engine"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewParserDebezium(inWrapped interface{}, _ bool, logger log.Logger, _ *stats.SourceStats) (parsers.Parser, error) {
	var srURL, username, password, namespaceID, tlsFile string
	switch in := inWrapped.(type) {
	case *ParserConfigDebeziumLb:
		srURL, username, password = in.SchemaRegistryURL, in.Username, in.Password
		tlsFile, namespaceID = in.TLSFile, in.NamespaceID
	case *ParserConfigDebeziumCommon:
		srURL, username, password = in.SchemaRegistryURL, in.Username, in.Password
		tlsFile, namespaceID = in.TLSFile, in.NamespaceID
	default:
		return nil, xerrors.Errorf("unknown parser config type '%T'", inWrapped)
	}

	buildDebeziumImpl := func(srURL, username, password string) (parsers.Parser, error) {
		var client *confluent.SchemaRegistryClient = nil
		var err error
		if srURL != "" {
			client, err = confluent.NewSchemaRegistryClientWithTransport(srURL, tlsFile, logger)
			if err != nil {
				return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
			}
			client.SetCredentials(username, password)
		}
		return debeziumengine.NewDebeziumImpl(logger, client, uint64(runtime.NumCPU()*4)), nil
	}

	if namespaceID != "" {
		ysrParser, err := parsers.WithYSRNamespaceIDs(func() (parsers.Parser, abstract.Expirer, error) {
			params, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
			if err != nil {
				return nil, nil, xerrors.Errorf("failed to resolve yandex schema registry namespace id: %w", err)
			}

			debeziumImpl, err := buildDebeziumImpl(params.URL, params.Username, params.Password)
			if err != nil {
				return nil, nil, xerrors.Errorf("failed to create debezium impl: %w", err)
			}

			return debeziumImpl, &params, nil
		}, namespaceID, logger)
		if err != nil {
			return nil, xerrors.Errorf("failed to create YSR parser: %w", err)
		}
		return ysrParser, nil
	}

	return buildDebeziumImpl(srURL, username, password)
}

func init() {
	parsers.Register(
		NewParserDebezium,
		[]parsers.AbstractParserConfig{new(ParserConfigDebeziumCommon), new(ParserConfigDebeziumLb)},
	)
}
