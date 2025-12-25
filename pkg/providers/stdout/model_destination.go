package stdout

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"go.uber.org/zap/zapcore"
)

type StdoutDestination struct {
	ShowData          bool              `log:"true"`
	TransformerConfig map[string]string `log:"true"`
	TriggingCount     int               `log:"true"`
	TriggingSize      uint64            `log:"true"`
	TriggingInterval  time.Duration     `log:"true"`
}

var _ model.Destination = (*StdoutDestination)(nil)

func (d *StdoutDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (StdoutDestination) WithDefaults() {
}

func (d *StdoutDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (d *StdoutDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (StdoutDestination) IsDestination() {
}

func (d *StdoutDestination) GetProviderType() abstract.ProviderType {
	return ProviderTypeStdout
}

func (d *StdoutDestination) Validate() error {
	return nil
}

func (d *StdoutDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.TriggingCount,
		TriggingSize:     d.TriggingSize,
		TriggingInterval: d.TriggingInterval,
	}
}
