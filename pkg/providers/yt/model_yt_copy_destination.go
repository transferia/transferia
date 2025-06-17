//go:build !disable_yt_provider

package yt

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/yt"
)

type YtCopyDestination struct {
	Cluster            string
	YtToken            string
	Prefix             string
	Parallelism        uint64
	Pool               string
	UsePushTransaction bool
	ResourceLimits     *spec.ResourceLimits
}

var _ model.Destination = (*YtCopyDestination)(nil)

func (y *YtCopyDestination) IsDestination() {}

func (y *YtCopyDestination) Transformer() map[string]string {
	return make(map[string]string)
}

func (y *YtCopyDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (y *YtCopyDestination) WithDefaults() {
	if y.Parallelism == 0 {
		y.Parallelism = 5
	}
	if y.ResourceLimits == nil {
		y.ResourceLimits = new(spec.ResourceLimits)
	}
	if y.ResourceLimits.UserSlots == 0 {
		y.ResourceLimits.UserSlots = 1000
	}
}

func (y *YtCopyDestination) GetProviderType() abstract.ProviderType {
	return CopyType
}

func (y *YtCopyDestination) Validate() error {
	if y.Parallelism == 0 {
		return xerrors.New("parallelism should not be 0")
	}
	if y.ResourceLimits == nil {
		return xerrors.New("ParserResource limits should be set")
	}
	return nil
}

func (y *YtCopyDestination) SupportMultiWorkers() bool {
	return false
}

func (y *YtCopyDestination) SupportMultiThreads() bool {
	return false
}

func (y *YtCopyDestination) Proxy() string {
	return y.Cluster
}

func (y *YtCopyDestination) Token() string {
	return y.YtToken
}

func (y *YtCopyDestination) DisableProxyDiscovery() bool {
	return false
}

func (y *YtCopyDestination) CompressionCodec() yt.ClientCompressionCodec {
	return yt.ClientCodecBrotliFastest
}
