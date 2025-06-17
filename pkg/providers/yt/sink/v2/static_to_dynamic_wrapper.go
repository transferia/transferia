//go:build !disable_yt_provider

package staticsink

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	dyn_sink "github.com/transferia/transferia/pkg/providers/yt/sink"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type sinker struct {
	ytClient             yt.Client
	config               yt2.YtDestinationModel
	staticSink           abstract.Sinker
	stateStorage         *ytStateStorage
	staticFinishedTables []ypath.Path
	dir                  ypath.Path
}

func NewStaticSinkWrapper(cfg yt2.YtDestinationModel, cp coordinator.Coordinator, transferID string, registry metrics.Registry, logger log.Logger) (abstract.Sinker, error) {
	staticSink, err := NewStaticSink(cfg, cp, transferID, registry, logger)
	if err != nil {
		return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
	}

	ytClient, err := ytclient.FromConnParams(cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("error getting YT Client: %w", err)
	}

	return &sinker{
		ytClient:             ytClient,
		config:               cfg,
		staticSink:           staticSink,
		staticFinishedTables: []ypath.Path{},
		stateStorage:         newYtStateStorage(cp, transferID, logger),
		dir:                  ypath.Path(cfg.Path()),
	}, nil
}

func (s *sinker) Close() error {
	return s.staticSink.Close()
}

func (s *sinker) Commit() error {
	commitSink, ok := s.staticSink.(abstract.Committable)
	if !ok {
		return xerrors.Errorf("static sink is not commitable for some reason")
	}
	if err := commitSink.Commit(); err != nil {
		return err
	}

	state, err := s.stateStorage.GetState()
	if err != nil {
		return xerrors.Errorf("unable to get state on commit: %w", err)
	}
	for _, tablePath := range state.Tables {
		if err := s.convertStaticToDynamic(context.TODO(), ypath.Path(tablePath)); err != nil {
			return xerrors.Errorf("unable to make table %v dynamic: %w", tablePath, err)
		}
	}
	if err := s.stateStorage.RemoveState(); err != nil {
		return xerrors.Errorf("unable to remove static stage state: %w", err)
	}
	return nil
}

func (s *sinker) convertStaticToDynamic(ctx context.Context, tableYPath ypath.Path) error {
	return backoff.Retry(func() error {
		alterOptions := yt.AlterTableOptions{
			Dynamic: util.TruePtr(),
		}
		if err := s.ytClient.AlterTable(ctx, tableYPath, &alterOptions); err != nil {
			return xerrors.Errorf("unable to alter destination table %q: %w", tableYPath, err)
		}

		dstInfo, err := yt2.GetNodeInfo(ctx, s.ytClient, tableYPath)
		if err != nil {
			return xerrors.Errorf("unable to get node info: %w", err)
		}
		attrs := dyn_sink.BuildDynamicAttrs(dyn_sink.GetCols(dstInfo.Attrs.Schema), s.config)
		if err = s.ytClient.MultisetAttributes(ctx, tableYPath.Attrs(), attrs, nil); err != nil {
			return xerrors.Errorf("unable to set destination attributes: %w", err)
		}

		if err := migrate.MountAndWait(ctx, s.ytClient, tableYPath); err != nil {
			return xerrors.Errorf("unable to mount destination table %q: %w", tableYPath, err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
}

func (s *sinker) Push(input []abstract.ChangeItem) error {
	if len(input) == 0 {
		return nil
	}
	item := input[0]

	switch item.Kind {
	case abstract.DoneShardedTableLoad:
		if item.TableSchema.Columns().KeysNum() == len(item.TableSchema.Columns()) {
			newColumns := append(item.TableSchema.Columns(), abstract.NewColSchema("__dummy", schema.TypeAny, false))
			input[0].TableSchema = abstract.NewTableSchema(newColumns)
		}
		if err := s.staticSink.Push(input); err != nil {
			return xerrors.Errorf("failed to process snapshot stage: %w", err)
		}

		tableYPath := yt2.SafeChild(s.dir, yt2.MakeTableName(item.TableID(), s.config.AltNames()))
		s.staticFinishedTables = append(s.staticFinishedTables, tableYPath)
		if err := s.stateStorage.SetState(s.staticFinishedTables); err != nil {
			return xerrors.Errorf("unable to set finished tables: %w", err)
		}
	default:
		if err := s.staticSink.Push(input); err != nil {
			return xerrors.Errorf("failed to process snapshot stage: %w", err)
		}
	}
	return nil
}
