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
	stagingFactory := func() model.Destination {
		return new(LfStagingDestination)
	}

	gobwrapper.RegisterName("*server.YtDestination", new(YtDestination))
	gobwrapper.RegisterName("*server.YtDestinationWrapper", new(YtDestinationWrapper))
	gobwrapper.RegisterName("*server.YtSource", new(YtSource))
	gobwrapper.RegisterName("*server.YtCopyDestination", new(YtCopyDestination))
	gobwrapper.RegisterName("*server.LfStagingDestination", new(LfStagingDestination))

	model.RegisterDestination(ProviderType, destinationFactory)
	model.RegisterDestination(StagingType, stagingFactory)
	model.RegisterDestination(CopyType, destinationCopyFactory)
	model.RegisterSource(ProviderType, func() model.Source {
		return new(YtSource)
	})

	abstract.RegisterProviderName(ProviderType, "YT")
	abstract.RegisterProviderName(StagingType, "Logfeller staging area")
	abstract.RegisterProviderName(CopyType, "YT Copy")

	abstract.RegisterSystemTables(TableWAL)
}

const (
	TableWAL = "__wal"

	ProviderType = abstract.ProviderType("yt")
	StagingType  = abstract.ProviderType("lfstaging")
	CopyType     = abstract.ProviderType("ytcopy")
)
