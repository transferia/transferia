package abstract

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
)

// TableIDParser parses a string representation of a table identifier into TableID.
// Different providers may use different formats (e.g. pg: schema.table, yt: //path/to/table).
type TableIDParser func(object string) (*TableID, error)

var tableIDParsers = map[ProviderType]TableIDParser{}

// RegisterTableIDParser registers a parser for the given provider type.
// Called from provider init() functions.
func RegisterTableIDParser(typ ProviderType, parser TableIDParser) {
	tableIDParsers[typ] = parser
}

// ParseTableIDForProvider parses object string using the parser registered for providerType.
// Falls back to NewTableIDFromString if no parser is registered.
func ParseTableIDForProvider(object string, providerType ProviderType) (*TableID, error) {
	if fn, ok := tableIDParsers[providerType]; ok {
		res, err := fn(object)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse table ID for provider %s: %s: %w", string(providerType), object, err)
		}
		return res, nil
	}
	return NewTableIDFromString(object)
}

// NewTableIDFromString is a fallback parses the given FQTN to construct a TableID.
// Normally, every provider should have its own parser registered via RegisterTableIDParser
func NewTableIDFromString(fqtn string) (*TableID, error) {
	parts, err := identifierToParts(fqtn)
	if err != nil {
		return nil, coded.Errorf(codes.InvalidObjectIdentifier, "failed to identify parts: %s: %w", fqtn, err)
	}

	switch len(parts) {
	case 0:
		return nil, coded.Errorf(codes.InvalidObjectIdentifier, "object identifier has no parts: %s", fqtn)
	case 1:
		return &TableID{Namespace: "", Name: parts[0]}, nil
	case 2:
		return &TableID{Namespace: parts[0], Name: parts[1]}, nil
	default:
		return &TableID{Namespace: "", Name: strings.Join(parts, ".")}, nil

	}
}

// NewTableIDFromStringPg parses the given FQTN in PostgreSQL syntax to construct a TableID.
func NewTableIDFromStringPg(fqtn string, replaceOmittedSchemaWithPublic bool) (*TableID, error) {
	parts, err := identifierToParts(fqtn)
	if err != nil {
		return nil, coded.Errorf(codes.InvalidObjectIdentifier, "failed to identify parts: %s: %w", fqtn, err)
	}

	switch len(parts) {
	case 0:
		return nil, coded.Errorf(codes.InvalidObjectIdentifier, "object identifier has no parts: %s", fqtn)
	case 1:
		if replaceOmittedSchemaWithPublic {
			return &TableID{Namespace: "public", Name: parts[0]}, nil
		}
		return &TableID{Namespace: "", Name: parts[0]}, nil
	case 2:
		return &TableID{Namespace: parts[0], Name: parts[1]}, nil
	default:
		return nil, coded.Errorf(codes.InvalidObjectIdentifier, "identifier '%s' contains %d parts instead of maximum two", fqtn, len(parts))

	}
}

func NewFullPathTableID(object string) (*TableID, error) {
	if object == "" {
		return nil, xerrors.New("empty table identifier")
	}
	return &TableID{Namespace: "", Name: object}, nil
}
