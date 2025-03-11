package typesystem

import (
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	RestPlaceholder         = "REST..."
	NotSupportedPlaceholder = "N/A"
)

type Rule struct {
	// `Target` stores the mapping of `schema.Type`-s to provider-specific types for each `Target` provider.
	// example:
	//	{schema.TypeString: "TEXT"}
	Target map[schema.Type]string
	// `Source` stores the conversion rules from the provider-specific type to the `schema.Type` for each `Source` provider.
	// example:
	//	{"TINYTEXT": schema.TypeString}
	Source map[string]schema.Type
}

var (
	// `sourceTypeRegistry` stores the conversion rules from the provider-specific type to the transfer type for each `Source` provider.
	// example:
	//	{MYSQL: {"TINYTEXT": schema.TypeString}}
	sourceTypeRegistry = map[abstract.ProviderType]map[string]schema.Type{}
	// `targetTypeRegistry` stores the mapping of transfer types to provider-specific types for each `Target` provider.
	// example:
	//	{MYSQL: {schema.TypeString: "TEXT"}}
	targetTypeRegistry = map[abstract.ProviderType]map[schema.Type]string{}
)

func SourceRules(provider abstract.ProviderType, rules map[schema.Type][]string) {
	sourceTypeRegistry[provider] = map[string]schema.Type{}
	for target, sources := range rules {
		for _, sourceTyp := range sources {
			sourceTypeRegistry[provider][sourceTyp] = target
		}
	}
}

func TargetRule(provider abstract.ProviderType, rules map[schema.Type]string) {
	targetTypeRegistry[provider] = rules
}

func SupportedTypes() []schema.Type {
	return []schema.Type{
		schema.TypeInt64,
		schema.TypeInt32,
		schema.TypeInt16,
		schema.TypeInt8,
		schema.TypeUint64,
		schema.TypeUint32,
		schema.TypeUint16,
		schema.TypeUint8,
		schema.TypeFloat32,
		schema.TypeFloat64,
		schema.TypeBytes,
		schema.TypeString,
		schema.TypeBoolean,
		schema.TypeDate,
		schema.TypeDatetime,
		schema.TypeTimestamp,
		schema.TypeAny,
	}
}

func RuleFor(provider abstract.ProviderType) Rule {
	return Rule{
		Target: targetTypeRegistry[provider],
		Source: sourceTypeRegistry[provider],
	}
}
