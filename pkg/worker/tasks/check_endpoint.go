package tasks

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/transferia/transferia/internal/logger"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

const checkStageTimeout = 5 * time.Minute

// CheckEndpoint checks the endpoint of the transfer (in Src or in Dst, the other side is nil) and reports every check
// to cp as soon as it finishes. Nothing is written.
// A failed check is reported, not returned; errors are internal: an undelivered report or a transfer without endpoints.
func CheckEndpoint(
	ctx context.Context, operationID string, transfer *model.Transfer,
	cp coordinator.CheckEndpointReporter, registry core_metrics.Registry,
) error {
	switch {
	case transfer.Src != nil:
		if err := checkSource(ctx, operationID, transfer, cp, registry); err != nil {
			return xerrors.Errorf("source: %w", err)
		}
	case transfer.Dst != nil:
		if err := checkDestination(ctx, operationID, transfer.Dst, cp); err != nil {
			return xerrors.Errorf("target: %w", err)
		}
	default:
		return xerrors.New("transfer has no endpoint to check")
	}
	return nil
}

// checkSource checks the connection, then lists the tables.
func checkSource(
	ctx context.Context, operationID string, transfer *model.Transfer,
	cp coordinator.CheckEndpointReporter, registry core_metrics.Registry,
) error {
	connErr := checkConnection(ctx, transfer.Src)
	if err := cp.ReportConnectionCheck(operationID, connErr); err != nil {
		return xerrors.Errorf("unable to report conn check: %w", err)
	}
	if connErr != nil {
		return nil
	}
	tables, listErr := listTables(ctx, transfer, registry)
	if listErr != nil {
		logger.Log.Error("source table listing failed", log.Error(listErr))
	}
	if err := cp.ReportListTables(operationID, tables, listErr); err != nil {
		return xerrors.Errorf("unable to report list tables: %w", err)
	}
	return nil
}

// checkDestination checks only the connection.
func checkDestination(
	ctx context.Context, operationID string, dst model.Destination, cp coordinator.CheckEndpointReporter,
) error {
	return cp.ReportConnectionCheck(operationID, checkConnection(ctx, dst))
}

func checkConnection(ctx context.Context, endpoint model.EndpointParams) error {
	checker, ok := endpoint.(model.ConnectionChecker)
	if !ok {
		return coded.Errorf(error_codes.CheckEndpointNotSupported, "connection check is not supported for this endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, checkStageTimeout)
	defer cancel()
	return checker.CheckConnection(ctx)
}

// listTables lists the source tables: the filters of the source apply, the ones of the transfer do not.
func listTables(
	ctx context.Context, transfer *model.Transfer, registry core_metrics.Registry,
) ([]abstract.TableID, error) {
	lister, ok := providers.Source[providers.TableLister](logger.Log, registry, coordinator.NewFakeClient(), transfer)
	if !ok {
		return nil, coded.Errorf(error_codes.CheckEndpointNotSupported, "table listing is not supported for this endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, checkStageTimeout)
	defer cancel()
	tables, err := lister.ListTables(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	if source, ok := transfer.Src.(abstract.Includeable); ok {
		tables = model.FilteredMap(tables, source)
	}
	ids := slices.Collect(maps.Keys(tables))
	slices.SortFunc(ids, abstract.TableID.Less)
	return ids, nil
}
