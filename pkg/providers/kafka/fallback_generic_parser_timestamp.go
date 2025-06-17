//go:build !disable_kafka_provider

package kafka

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	jsonengine "github.com/transferia/transferia/pkg/parsers/registry/json/engine"
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
