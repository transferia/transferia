package kafka

import (
	"github.com/transferria/transferria/pkg/abstract/typesystem"
	jsonengine "github.com/transferria/transferria/pkg/parsers/registry/json/engine"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       4,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: jsonengine.GenericParserTimestampFallback,
		}
	})
}
