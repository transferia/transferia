package coordinator_utils

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"go.ytsaurus.tech/library/go/core/log"
)

func BuildShardingContext(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	cfg *s3.S3Source,
) ([]byte, error) {
	startTime := time.Now()

	objects, err := list.ListAll(ctx, logger, registry, cfg)
	if err != nil {
		return nil, xerrors.Errorf("failed to list all objects, err: %w", err)
	}

	listTime := time.Since(startTime)

	resuit, err := r_window.NewRWindowFromFiles(cfg.OverlapDuration, listTime+cfg.OverlapDuration, objects)
	if err != nil {
		return nil, xerrors.Errorf("failed to create lr window, err: %w", err)
	}
	serialized, err := resuit.Serialize()
	if err != nil {
		return nil, xerrors.Errorf("failed to serialize resuit, err: %w", err)
	}
	return serialized, nil
}
