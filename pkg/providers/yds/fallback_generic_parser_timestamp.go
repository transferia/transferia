package yds

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	json_engine "github.com/transferia/transferia/pkg/parsers/registry/json/engine"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       4,
			Picker:   typesystem.ProviderType(YDSProviderType),
			Function: json_engine.GenericParserTimestampFallback,
		}
	})
}
