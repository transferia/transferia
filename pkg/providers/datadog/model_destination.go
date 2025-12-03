package datadog

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

type DatadogDestination struct {
	ClientAPIKey model.SecretString
	DatadogHost  string `log:"true"`

	// mapping to columns
	SourceColumn    string   `log:"true"`
	TagColumns      []string `log:"true"`
	HostColumn      string   `log:"true"`
	ServiceColumn   string   `log:"true"`
	MessageTemplate string   `log:"true"`
	ChunkSize       int      `log:"true"`
}

var _ model.Destination = (*DatadogDestination)(nil)

func (d *DatadogDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *DatadogDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *DatadogDestination) Validate() error {
	return nil
}

func (d *DatadogDestination) WithDefaults() {
	if d.ChunkSize == 0 {
		d.ChunkSize = 500
	}
}

func (d *DatadogDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (d *DatadogDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	if _, ok := src.(model.AppendOnlySource); ok {
		return nil
	}
	return xerrors.Errorf("%T is not compatible with Datadog, only append only source allowed", src)
}

func (d *DatadogDestination) IsDestination() {
}
