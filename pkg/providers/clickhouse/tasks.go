//go:build !disable_clickhouse_provider

package clickhouse

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	sink_factory "github.com/transferia/transferia/pkg/sink"
)

func (p *Provider) loadClickHouseSchema() error {
	if _, ok := p.transfer.Src.(*model.ChSource); !ok {
		return nil
	}
	if _, ok := p.transfer.Dst.(*model.ChDestination); !ok {
		return nil
	}
	sink, err := sink_factory.MakeAsyncSink(p.transfer, p.logger, p.registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	storage, err := p.Storage()
	if err != nil {
		return xerrors.Errorf("failed to resolve storage: %w", err)
	}
	defer storage.Close()
	tables, err := dp_model.FilteredTableList(storage, p.transfer)
	if err != nil {
		return xerrors.Errorf("failed to list tables and their schemas: %w", err)
	}
	chStorage := storage.(*Storage)
	if err := chStorage.CopySchema(tables, abstract.PusherFromAsyncSink(sink)); err != nil {
		return xerrors.Errorf("unable to copy clickhouse schema: %w", err)
	}
	return nil
}
