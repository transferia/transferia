package yt

import (
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
)

type YtStorageParams struct {
	Token                 string
	Cluster               string                 `log:"true"`
	Path                  string                 `log:"true"`
	Spec                  map[string]interface{} `log:"true"`
	DisableProxyDiscovery bool                   `log:"true"`
	ConnParams            yt_client.ConnParams
}

func (d *YtDestination) ToStorageParams() *YtStorageParams {
	return &YtStorageParams{
		Token:                 d.Token,
		Cluster:               d.Cluster,
		Path:                  d.Path,
		Spec:                  nil,
		DisableProxyDiscovery: d.Connection.DisableProxyDiscovery,
		ConnParams:            nil,
	}
}
