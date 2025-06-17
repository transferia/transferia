//go:build !disable_yt_provider

package yt

type YtStorageParams struct {
	Token                 string
	Cluster               string
	Path                  string
	Spec                  map[string]interface{}
	DisableProxyDiscovery bool
}

func (d *YtDestination) ToStorageParams() *YtStorageParams {
	return &YtStorageParams{
		Token:                 d.Token,
		Cluster:               d.Cluster,
		Path:                  d.Path,
		Spec:                  nil,
		DisableProxyDiscovery: d.Connection.DisableProxyDiscovery,
	}
}
