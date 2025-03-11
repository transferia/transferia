package yt

import (
	"encoding/gob"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
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

	gob.RegisterName("*server.YtDestination", new(YtDestination))
	gob.RegisterName("*server.YtDestinationWrapper", new(YtDestinationWrapper))
	gob.RegisterName("*server.YtSource", new(YtSource))
	gob.RegisterName("*server.YtCopyDestination", new(YtCopyDestination))
	gob.RegisterName("*server.LfStagingDestination", new(LfStagingDestination))

	model.RegisterDestination(ProviderType, destinationFactory)
	model.RegisterDestination(StagingType, stagingFactory)
	model.RegisterDestination(CopyType, destinationCopyFactory)
	model.RegisterSource(ProviderType, func() model.Source {
		return new(YtSource)
	})

	abstract.RegisterProviderName(ProviderType, "YT")
	abstract.RegisterProviderName(StagingType, "Logfeller staging area")
	abstract.RegisterProviderName(CopyType, "YT Copy")

	abstract.RegisterSystemTables(TableProgressRelativePath, TableWAL)
}

const (
	TableProgressRelativePath = abstract.TableLSN // "__data_transfer_lsn"
	TableWAL                  = "__wal"

	ProviderType = abstract.ProviderType("yt")
	StagingType  = abstract.ProviderType("lfstaging")
	CopyType     = abstract.ProviderType("ytcopy")
)
