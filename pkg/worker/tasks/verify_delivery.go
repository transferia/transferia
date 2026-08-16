package tasks

import (
	"context"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/sink_factory"
	"go.ytsaurus.tech/library/go/core/log"
)

func VerifyDelivery(ctx context.Context, transfer model.Transfer, lgr log.Logger, registry core_metrics.Registry) error {
	switch dst := transfer.Dst.(type) {
	case *provider_postgres.PgDestination:
		// _ping and other tables created if MaintainTables is set to true
		dstMaintainTables := dst.MaintainTables
		dst.MaintainTables = true

		// restoring destination's MaintainTables value
		defer func() {
			dst.MaintainTables = dstMaintainTables
		}()
	}
	sink, err := sink_factory.MakeAsyncSink(&transfer, new(model.TransferOperation), lgr, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	if err := pingSinker(ctx, sink); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to ping sinker: %w", err)
	}

	factory, ok := providers.Source[providers.Verifier](lgr, registry, coordinator.NewFakeClient(), &transfer)
	if !ok {
		return nil
	}
	return factory.Verify(ctx)
}

func pingSinker(ctx context.Context, s abstract.AsyncSink) error {
	dropItem := []abstract.ChangeItem{
		{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.DropTableKind,
			Table:        "_ping",
			ColumnValues: []interface{}{"_ping"},
		},
	}

	err := waitAsyncPush(ctx, s.AsyncPush(dropItem))
	if err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	err = waitAsyncPush(ctx, s.AsyncPush([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "_ping",
			ColumnNames:  []string{"k", "_dummy"},
			ColumnValues: []interface{}{1, "nothing"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "k",
					DataType:   "int32",
					PrimaryKey: true,
				}, {
					ColumnName: "_dummy",
					DataType:   "string",
				},
			}),
		},
	}))
	if err != nil {
		return xerrors.Errorf("unable to push: %w", err)
	}

	if err := waitAsyncPush(ctx, s.AsyncPush(dropItem)); err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	return nil
}

func waitAsyncPush(ctx context.Context, result <-chan error) error {
	select {
	case err := <-result:
		//nolint:descriptiveerrors
		return err
	case <-ctx.Done():
		//nolint:descriptiveerrors
		return ctx.Err()
	}
}
