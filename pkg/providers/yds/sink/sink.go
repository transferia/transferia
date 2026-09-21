package sink

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ydb_topics_sink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSink(cfg sinkConfig, registry core_metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	if err := cfg.prepareConfig(); err != nil {
		return nil, xerrors.Errorf("unable to prepare destination: %w", err)
	}
	topicSinkConfig, err := cfg.topicSinkConfig()
	if err != nil {
		return nil, xerrors.Errorf("unable to build topic sink config: %w", err)
	}

	return ydb_topics_sink.NewReplicationSink(
		topicSinkConfig,
		registry,
		lgr,
		transferID,
	)
}
