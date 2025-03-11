package fallback

import (
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/abstract/typesystem"
	"github.com/transferria/transferria/pkg/providers/yt"
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
