package yt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYtClusterProxy(t *testing.T) {
	for _, cluster := range []string{"hahn", "arnold", "localhost:1234", "lazarus.yt.yandex.net"} {
		t.Run(cluster, func(t *testing.T) {
			expectedProxy := cluster
			if cluster == "hahn" {
				expectedProxy = "lazarus.yt.yandex.net"
			}

			source := YtSource{Cluster: cluster}
			destination := NewYtDestinationV1(YtDestination{Cluster: cluster})
			copyDestination := YtCopyDestination{Cluster: cluster}
			require.Equal(t, expectedProxy, source.Proxy())
			require.Equal(t, expectedProxy, destination.Proxy())
			require.Equal(t, expectedProxy, copyDestination.Proxy())
			require.Equal(t, cluster, source.GetCluster())
			require.Equal(t, cluster, destination.Cluster())
			require.Equal(t, cluster, copyDestination.Cluster)
		})
	}
}

func TestYtSourceProxyOverride(t *testing.T) {
	for _, cluster := range []string{"hahn", "arnold", ""} {
		t.Run(cluster, func(t *testing.T) {
			source := YtSource{Cluster: cluster, YtProxy: "custom-proxy:1234"}
			expectedProxy := source.YtProxy
			if cluster == "hahn" {
				expectedProxy = "lazarus.yt.yandex.net"
			}
			require.Equal(t, expectedProxy, source.Proxy())
			require.Equal(t, "custom-proxy:1234", source.YtProxy)
		})
	}
}
