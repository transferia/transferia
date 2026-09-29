package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// CheckEndpoint runs the check stages for every endpoint of the transfer, nothing is read or written.
// Stages and their order are defined here; an endpoint only implements the optional model interfaces it supports.
func CheckEndpoint(ctx context.Context, transfer *model.Transfer) error {
	if transfer.Src != nil {
		if err := checkConnection(ctx, transfer.Src); err != nil {
			return xerrors.Errorf("source: %w", err)
		}
	}
	if transfer.Dst != nil {
		if err := checkConnection(ctx, transfer.Dst); err != nil {
			return xerrors.Errorf("target: %w", err)
		}
	}
	return nil
}

func checkConnection(ctx context.Context, endpoint model.EndpointParams) error {
	checker, ok := endpoint.(model.ConnectionChecker)
	if !ok {
		return xerrors.Errorf("connection check is not supported for %s endpoints", endpoint.GetProviderType())
	}
	if err := checker.CheckConnection(ctx); err != nil {
		return xerrors.Errorf("%s: %w", endpoint.GetProviderType(), err)
	}
	return nil
}
