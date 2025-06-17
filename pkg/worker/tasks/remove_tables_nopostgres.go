//go:build disable_postgres_provider

package tasks

import (
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func removeTableHandleSrc(cp coordinator.Coordinator, transfer model.Transfer, src model.Source, tables []string) error {
	return nil
}
