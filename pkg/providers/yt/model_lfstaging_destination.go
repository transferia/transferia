//go:build !disable_yt_provider

package yt

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

var _ model.Destination = (*LfStagingDestination)(nil)

type LfStagingDestination struct {
	Cluster           string
	Topic             string
	YtAccount         string
	LogfellerHomePath string
	TmpBasePath       string

	AggregationPeriod time.Duration

	SecondsPerTmpTable int64
	BytesPerTmpTable   int64

	YtToken string

	UsePersistentIntermediateTables bool
	UseNewMetadataFlow              bool
	MergeYtPool                     string
}

func (d *LfStagingDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (d *LfStagingDestination) Transformer() map[string]string {
	return map[string]string{}
}

func (d *LfStagingDestination) WithDefaults() {

	if d.AggregationPeriod == 0 {
		d.AggregationPeriod = time.Minute * 5
	}

	if d.SecondsPerTmpTable == 0 {
		d.SecondsPerTmpTable = 10
	}

	if d.BytesPerTmpTable == 0 {
		d.BytesPerTmpTable = 20 * 1024 * 1024
	}
}

func (LfStagingDestination) IsDestination() {
}

func (d *LfStagingDestination) GetProviderType() abstract.ProviderType {
	return StagingType
}

func (d *LfStagingDestination) Validate() error {
	return nil
}
