package coralogix

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

type CoralogixDestination struct {
	Token  model.SecretString
	Domain string `log:"true"`

	MessageTemplate string `log:"true"`
	ChunkSize       int    `log:"true"`
	SubsystemColumn string `log:"true"`
	ApplicationName string `log:"true"`

	// mapping to columns
	TimestampColumn string              `log:"true"`
	SourceColumn    string              `log:"true"`
	CategoryColumn  string              `log:"true"`
	ClassColumn     string              `log:"true"`
	MethodColumn    string              `log:"true"`
	ThreadIDColumn  string              `log:"true"`
	SeverityColumn  string              `log:"true"`
	HostColumn      string              `log:"true"`
	KnownSevereties map[string]Severity `log:"true"`
}

func (d *CoralogixDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *CoralogixDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *CoralogixDestination) Validate() error {
	return nil
}

func (d *CoralogixDestination) WithDefaults() {
	if d.ChunkSize == 0 {
		d.ChunkSize = 500
	}
}

func (d *CoralogixDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (d *CoralogixDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	if _, ok := src.(model.AppendOnlySource); ok {
		return nil
	}
	return xerrors.Errorf("%T is not compatible with Coralogix, only append only source allowed", src)
}

func (d *CoralogixDestination) IsDestination() {
}
