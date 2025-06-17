//go:build !disable_yt_provider

package fallback

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/providers/yt"
)

func init() {
	typesystem.AddFallbackTargetFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To: 9,
			Picker: func(endpoint model.EndpointParams) bool {
				if endpoint.GetProviderType() != yt.ProviderType {
					return false
				}

				dstParams, ok := endpoint.(*yt.YtDestinationWrapper)
				if !ok {
					return false
				}
				return dstParams.Static()
			},
			Function: func(item *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if item.Schema == "" {
					item.Table = "_" + item.Table
				}
				return item, nil
			},
		}
	})
}
