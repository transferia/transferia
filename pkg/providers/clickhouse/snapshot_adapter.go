package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
)

var (
	_ providers.SnapshotSinker          = (*Provider)(nil)
	_ providers.DstCleanuper            = (*Provider)(nil)
	_ providers.SnapshotOperationCloser = (*Provider)(nil)
)

func (p *Provider) CloseSnapshotOperation() {
	if !p.transfer.Dst.(*clickhouse_model.ChDestination).ExactlyOnceSnapshot || p.operation == nil {
		return
	}
	snapshot.StopStagingDDLKeepalives(p.transfer.ID, p.operation.OperationID)
}

func (p *Provider) SnapshotSink(config middlewares.Config) (abstract.Sinker, error) {
	dst := p.transfer.Dst.(*clickhouse_model.ChDestination)
	if !dst.ExactlyOnceSnapshot {
		return p.Sink(config)
	}
	ctx := context.Background()
	sink, err := p.newSnapshotSink(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("create exactly-once snapshot sink: %w", err)
	}
	if p.transfer.IsMain() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := sink.ResetPreviousOperations(ctx); err != nil {
			_ = sink.Close()
			return nil, fmt.Errorf("reset previous snapshot: %w", err)
		}
	}
	return sink, nil
}

func (p *Provider) newSnapshotSink(ctx context.Context, cleanup bool) (*snapshot.Sink, error) {
	operationID := ""
	if p.operation != nil {
		operationID = p.operation.OperationID
	}
	return snapshot.NewSinkFromTransfer(
		ctx,
		p.logger,
		p.registry,
		p.cp,
		p.transfer,
		operationID,
		cleanup,
		func(params clickhouse_model.ChSinkParams, version semver.Version) (func(abstract.ChangeItem) string, func(abstract.ChangeItem) (string, error)) {
			namer := new(sinkShard)
			namer.config, namer.altNames = params, MakeAltNames(params)
			buildName := func(item abstract.ChangeItem) string { return normalizeTableName(namer.tableName(item)) }
			buildDDL := func(item abstract.ChangeItem) (string, error) {
				if item.TableSchema == nil {
					return "", xerrors.New("missing snapshot table schema")
				}
				table := new(sinkTable)
				table.tableName, table.config, table.version = buildName(item), params, version
				schema := NewSchema(item.TableSchema.Columns(), params.SystemColumnsFirst(), table.tableName)
				return table.generateDDL(schema.abstractCols(), false), nil
			}
			return buildName, buildDDL
		},
	)
}

func (p *Provider) CleanupDestination(ctx context.Context) error {
	if !p.transfer.Dst.(*clickhouse_model.ChDestination).ExactlyOnceSnapshot {
		return nil
	}
	if _, homogeneous := p.transfer.Src.(*clickhouse_model.ChSource); homogeneous {
		// Homogeneous snapshots use the abstract2 target and have no publication journal.
		return nil
	}
	for {
		keys, err := p.cp.GetTransferStateKeys(p.transfer.ID)
		if err == nil {
			pending := false
			for _, key := range keys {
				pending = pending || strings.HasPrefix(key, transfer_state.StatePrefix)
			}
			if !pending {
				return nil
			}
		} else {
			return xerrors.Errorf("read ClickHouse snapshot cleanup journal: %w", err)
		}
		sink, err := p.newSnapshotSink(ctx, true)
		if err == nil {
			err = sink.Cleanup(ctx)
			_ = sink.Close()
		}
		if err == nil {
			return nil
		}
		p.logger.Warnf("ClickHouse snapshot cleanup is still pending: %v", err)
		select {
		case <-ctx.Done():
			return xerrors.Errorf("snapshot cleanup interrupted: %w", ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}
