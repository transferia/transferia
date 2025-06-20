//go:build !disable_bigquery_provider

package bigquery

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

var _ model.Destination = (*BigQueryDestination)(nil)

type BigQueryDestination struct {
	ProjectID     string
	Dataset       string
	Creds         string
	CleanupPolicy model.CleanupType
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
