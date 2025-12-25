package yt

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

var _ model.Destination = (*LfStagingDestination)(nil)

type LfStagingDestination struct {
	Cluster           string `log:"true"`
	Topic             string `log:"true"`
	YtAccount         string `log:"true"`
	LogfellerHomePath string `log:"true"`
	TmpBasePath       string `log:"true"`

	AggregationPeriod time.Duration `log:"true"`

	SecondsPerTmpTable int64 `log:"true"`
	BytesPerTmpTable   int64 `log:"true"`

	YtToken string

	UsePersistentIntermediateTables bool   `log:"true"`
	UseNewMetadataFlow              bool   `log:"true"`
	MergeYtPool                     string `log:"true"`
}

func (d *LfStagingDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
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
