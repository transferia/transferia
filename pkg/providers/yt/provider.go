package yt

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

func init() {
	destinationFactory := func() model.Destination {
		return &YtDestinationWrapper{
			Model:    new(YtDestination),
			_pushWal: false,
		}
	}
	destinationCopyFactory := func() model.Destination {
		return new(YtCopyDestination)
	}
	destinationManagedDynamicFactory := func() model.Destination {
		return new(YTSaurusDynamicDestination)
	}
	destinationManagedStaticFactory := func() model.Destination {
		return new(YTSaurusStaticDestination)
	}
	stagingFactory := func() model.Destination {
		return new(LfStagingDestination)
	}

	gobwrapper.RegisterName("*server.YtDestination", new(YtDestination))
	gobwrapper.RegisterName("*server.YtDestinationWrapper", new(YtDestinationWrapper))
	gobwrapper.RegisterName("*server.YtSource", new(YtSource))
	gobwrapper.RegisterName("*server.YTSaurusSource", new(YTSaurusSource))
	gobwrapper.RegisterName("*server.YtCopyDestination", new(YtCopyDestination))
	gobwrapper.RegisterName("*server.LfStagingDestination", new(LfStagingDestination))
	gobwrapper.RegisterName("*server.YTSaurusStaticDestination", new(YTSaurusStaticDestination))
	gobwrapper.RegisterName("*server.YTSaurusDynamicDestination", new(YTSaurusDynamicDestination))

	model.RegisterDestination(ManagedStaticProviderType, destinationManagedStaticFactory)
	model.RegisterDestination(ManagedDynamicProviderType, destinationManagedDynamicFactory)
	model.RegisterDestination(ProviderType, destinationFactory)
	model.RegisterDestination(StagingType, stagingFactory)
	model.RegisterDestination(CopyType, destinationCopyFactory)
	model.RegisterSource(ProviderType, func() model.Source {
		return new(YtSource)
	})
	model.RegisterSource(ManagedProviderType, func() model.Source {
		return new(YTSaurusSource)
	})

	abstract.RegisterProviderName(ProviderType, "YT")
	abstract.RegisterProviderName(StagingType, "Logfeller staging area")
	abstract.RegisterProviderName(CopyType, "YT Copy")
	abstract.RegisterProviderName(ManagedProviderType, "YTSaurus")
	abstract.RegisterProviderName(ManagedDynamicProviderType, "YTSaurus Dynamic")
	abstract.RegisterProviderName(ManagedStaticProviderType, "YTSaurus Static")

	abstract.RegisterSystemTables(TableWAL)
}

const (
	TableWAL = "__wal"

	ProviderType               = abstract.ProviderType("yt")
	StagingType                = abstract.ProviderType("lfstaging")
	CopyType                   = abstract.ProviderType("ytcopy")
	ManagedProviderType        = abstract.ProviderType("ytsaurus")
	ManagedStaticProviderType  = abstract.ProviderType("ytsaurus static")
	ManagedDynamicProviderType = abstract.ProviderType("ytsaurus dynamic")
)
