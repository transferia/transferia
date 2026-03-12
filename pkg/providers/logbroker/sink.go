package logbroker

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	topicsink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewReplicationSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return topicsink.NewReplicationSink(cfg.TopicSinkConfig(), registry, lgr, transferID)
}

func NewSnapshotSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return topicsink.NewSnapshotSink(cfg.TopicSinkConfig(), registry, lgr, transferID)
}
