//go:build !disable_postgres_provider

package tasks

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/sink"
)

func TestTargetEndpoint(transfer *model.Transfer) error {
	switch dst := transfer.Dst.(type) {
	case *postgres.PgDestination:
		// _ping and other tables created if MaintainTables is set to true
		dstMaintainTables := dst.MaintainTables
		dst.MaintainTables = true

		// restoring destination's MaintainTables value
		defer func() {
			dst.MaintainTables = dstMaintainTables
		}()
	}
	sink, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	return pingSinker(sink)
}
