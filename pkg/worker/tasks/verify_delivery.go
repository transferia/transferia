package tasks

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func VerifyDelivery(transfer model.Transfer, lgr log.Logger, registry metrics.Registry) error {
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
	sink, err := sink.MakeAsyncSink(&transfer, lgr, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	if err := pingSinker(sink); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to ping sinker: %w", err)
	}

	factory, ok := providers.Source[providers.Verifier](lgr, registry, coordinator.NewFakeClient(), &transfer)
	if !ok {
		return nil
	}
	return factory.Verify(context.TODO())
}

func pingSinker(s abstract.AsyncSink) error {
	dropItem := []abstract.ChangeItem{
		{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.DropTableKind,
			Table:        "_ping",
			ColumnValues: []interface{}{"_ping"},
		},
	}

	err := <-s.AsyncPush(dropItem)
	if err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	err = <-s.AsyncPush([]abstract.ChangeItem{
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
	})
	if err != nil {
		return xerrors.Errorf("unable to push: %w", err)
	}

	if err := <-s.AsyncPush(dropItem); err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	return nil
}
