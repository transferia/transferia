package bigquery

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.uber.org/zap/zapcore"
)

var _ model.Destination = (*BigQueryDestination)(nil)

type BigQueryDestination struct {
	ProjectID     string `log:"true"`
	Dataset       string `log:"true"`
	Creds         string
	CleanupPolicy model.CleanupType `log:"true"`
}

func (b *BigQueryDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(b, enc)
}

func (b *BigQueryDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (b *BigQueryDestination) Validate() error {
	return nil
}

func (b *BigQueryDestination) WithDefaults() {
	if b.CleanupPolicy == "" {
		b.CleanupPolicy = model.Drop
	}
}

func (b *BigQueryDestination) CleanupMode() model.CleanupType {
	return b.CleanupPolicy
}

func (b *BigQueryDestination) IsDestination() {}
