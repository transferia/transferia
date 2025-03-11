package stdout

import (
	"time"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares/async/bufferer"
)

type StdoutDestination struct {
	ShowData          bool
	TransformerConfig map[string]string
	TriggingCount     int
	TriggingSize      uint64
	TriggingInterval  time.Duration
}

var _ model.Destination = (*StdoutDestination)(nil)

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

func (d *StdoutDestination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    d.TriggingCount,
		TriggingSize:     d.TriggingSize,
		TriggingInterval: d.TriggingInterval,
	}
}
