package topicwriter

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type Writer interface {
	Write(ctx context.Context, data []byte) error
	Close() error
}

func NewWriter(logger log.Logger, cfg *Config) (Writer, error) {
	if cfg.UseFederation {
		fedWriter, err := newFedYDBWriter(logger, cfg)
		if err != nil {
			return nil, xerrors.Errorf("unable to create federation writer: %w", err)
		}
		return fedWriter, nil
	}
	return newYDBWriter(logger, cfg)
}
